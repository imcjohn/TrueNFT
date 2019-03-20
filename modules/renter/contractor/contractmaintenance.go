package contractor

// contractmaintenance.go handles forming and renewing contracts for the
// contractor. This includes deciding when new contracts need to be formed, when
// contracts need to be renewed, and if contracts need to be blacklisted.

import (
	"fmt"
	"math/big"
	"reflect"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/renter/proto"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/fastrand"

	"gitlab.com/NebulousLabs/errors"
)

var (
	// ErrInsufficientAllowance indicates that the renter's allowance is less
	// than the amount necessary to store at least one sector
	ErrInsufficientAllowance = errors.New("allowance is not large enough to cover fees of contract creation")
	errTooExpensive          = errors.New("host price was too high")
)

type (
	// fileContractRenewal is an instruction to renew a file contract.
	fileContractRenewal struct {
		id     types.FileContractID
		amount types.Currency
	}
)

// managedCheckForDuplicates checks for static contracts that have the same host
// key and moves the older one to old contracts.
func (c *Contractor) managedCheckForDuplicates() {
	// Build map for comparison.
	pubkeys := make(map[string]types.FileContractID)
	var newContract, oldContract modules.RenterContract
	for _, contract := range c.staticContracts.ViewAll() {
		id, exists := pubkeys[contract.HostPublicKey.String()]
		if !exists {
			pubkeys[contract.HostPublicKey.String()] = contract.ID
			continue
		}

		// Duplicate contract found, determine older contract to delete.
		if rc, ok := c.staticContracts.View(id); ok {
			if rc.StartHeight >= contract.StartHeight {
				newContract, oldContract = rc, contract
			} else {
				newContract, oldContract = contract, rc
			}
			c.log.Printf("Duplicate contract found. New conract is %x and old contract is %v", newContract.ID, oldContract.ID)

			// Get SafeContract
			oldSC, ok := c.staticContracts.Acquire(oldContract.ID)
			if !ok {
				// Update map
				pubkeys[contract.HostPublicKey.String()] = newContract.ID
				continue
			}

			// Link the contracts to each other and then store the old contract
			// in the record of historic contracts.
			//
			// TODO: This code assumes that the contracts are linked because
			// they share a host, but it's not guaranteed that just be cause two
			// contracts have the same host that one is a renewal of the other.
			// This code also ensures that a renter can only ever have a single
			// contract with a host, which is not a restriction that we want to
			// be locked into.
			c.mu.Lock()
			c.renewedFrom[newContract.ID] = oldContract.ID
			c.renewedTo[oldContract.ID] = newContract.ID
			c.oldContracts[oldContract.ID] = oldSC.Metadata()
			c.pubKeysToContractID[string(newContract.HostPublicKey.Key)] = newContract.ID

			// Save the contractor and delete the contract.
			//
			// TODO: Ideally these two things would happen atomically, but I'm
			// not completely certain that's feasible with our current
			// architecture.
			err := c.saveSync()
			if err != nil {
				c.log.Println("Failed to save the contractor after updating renewed maps.")
			}
			c.mu.Unlock()
			c.staticContracts.Delete(oldSC)

			// Update the pubkeys map to contain the newest contract id.
			//
			// TODO: This means that if there are multiple duplicates, say 3
			// contracts that all share the same host, then the ordering may not
			// be perfect. If in reality the renewal order was A<->B<->C, it's
			// possible for the contractor to end up with A->C and B<->C in the
			// mapping.
			pubkeys[contract.HostPublicKey.String()] = newContract.ID
		}
	}
}

// managedEstimateRenewFundingRequirements estimates the amount of money that a
// contract is going to need in the next billing cycle by looking at how much
// storage is in the contract and what the historic usage pattern of the
// contract has been.
func (c *Contractor) managedEstimateRenewFundingRequirements(contract modules.RenterContract, blockHeight types.BlockHeight, allowance modules.Allowance) (types.Currency, error) {
	// Fetch the host pricing to use in the estimate.
	host, exists := c.hdb.Host(contract.HostPublicKey)
	if !exists {
		return types.ZeroCurrency, errors.New("could not find host in hostdb")
	}
	if host.Filtered {
		return types.ZeroCurrency, errors.New("host is blacklisted")
	}

	// Estimate the amount of money that's going to be needed for existing
	// storage.
	dataStored := contract.Transaction.FileContractRevisions[0].NewFileSize
	maintenanceCost := types.NewCurrency64(dataStored).Mul64(uint64(allowance.Period)).Mul(host.StoragePrice)

	// For the upload and download estimates, we're going to need to know the
	// amount of money that was spent on upload and download by this contract
	// line in this period. That's going to require iterating over the renew
	// history of the contract to get all the spending across any refreshes that
	// ocurred this period.
	prevUploadSpending := contract.UploadSpending
	prevDownloadSpending := contract.DownloadSpending
	c.mu.Lock()
	currentID := contract.ID
	for i := 0; i < 10e3; i++ { // prevent an infinite loop if there's an [impossible] contract cycle
		// If there is no previous contract, nothing to do.
		var exists bool
		currentID, exists = c.renewedFrom[currentID]
		if !exists {
			break
		}

		// If the contract is not in oldContracts, that's probably a bug, but
		// nothing to do otherwise.
		currentContract, exists := c.oldContracts[currentID]
		if !exists {
			c.log.Println("WARN: A known previous contract is not found in c.oldContracts")
			break
		}

		// If the contract did not start in the current period, then it is not
		// relevant, and none of the previous contracts will be relevant either.
		if currentContract.StartHeight < c.currentPeriod {
			break
		}

		// Add the upload and download spending.
		prevUploadSpending = prevUploadSpending.Add(currentContract.UploadSpending)
		prevDownloadSpending = prevDownloadSpending.Add(currentContract.DownloadSpending)
	}
	c.mu.Unlock()

	// Estimate the amount of money that's going to be needed for new storage
	// based on the amount of new storage added in the previous period. Account
	// for both the storage price as well as the upload price.
	prevUploadDataEstimate := prevUploadSpending
	if !host.UploadBandwidthPrice.IsZero() {
		// TODO: Because the host upload bandwidth price can change, this is not
		// the best way to estimate the amount of data that was uploaded to this
		// contract. Better would be to look at the amount of data stored in the
		// contract from the previous cycle and use that to determine how much
		// total data.
		prevUploadDataEstimate = prevUploadDataEstimate.Div(host.UploadBandwidthPrice)
	}
	// Sanity check - the host may have changed prices, make sure we aren't
	// assuming an unreasonable amount of data.
	if types.NewCurrency64(dataStored).Cmp(prevUploadDataEstimate) < 0 {
		prevUploadDataEstimate = types.NewCurrency64(dataStored)
	}
	// The estimated cost for new upload spending is the previous upload
	// bandwidth plus the implied storage cost for all of the new data.
	newUploadsCost := prevUploadSpending.Add(prevUploadDataEstimate.Mul64(uint64(allowance.Period)).Mul(host.StoragePrice))

	// The download cost is assumed to be the same. Even if the user is
	// uploading more data, the expectation is that the download amounts will be
	// relatively constant. Add in the contract price as well.
	newDownloadsCost := prevDownloadSpending
	contractPrice := host.ContractPrice

	// Aggregate all estimates so far to compute the estimated siafunds fees.
	// The transaction fees are not included in the siafunds estimate because
	// users are not charged siafund fees on money that doesn't go into the file
	// contract (and the transaction fee goes to the miners, not the file
	// contract).
	beforeSiafundFeesEstimate := maintenanceCost.Add(newUploadsCost).Add(newDownloadsCost).Add(contractPrice)
	afterSiafundFeesEstimate := types.Tax(blockHeight, beforeSiafundFeesEstimate).Add(beforeSiafundFeesEstimate)

	// Get an estimate for how much money we will be charged before going into
	// the transaction pool.
	_, maxTxnFee := c.tpool.FeeEstimation()
	txnFees := maxTxnFee.Mul64(modules.EstimatedFileContractTransactionSetSize)

	// Add them all up and then return the estimate plus 33% for error margin
	// and just general volatility of usage pattern.
	estimatedCost := afterSiafundFeesEstimate.Add(txnFees)
	estimatedCost = estimatedCost.Add(estimatedCost.Div64(3))

	// Check for a sane minimum. The contractor should not be forming contracts
	// with less than 'fileContractMinimumFunding / (num contracts)' of the
	// value of the allowance.
	minimum := allowance.Funds.MulFloat(fileContractMinimumFunding).Div64(allowance.Hosts)
	if estimatedCost.Cmp(minimum) < 0 {
		estimatedCost = minimum
	}
	return estimatedCost, nil
}

// managedInterruptContractMaintenance will issue an interrupt signal to any
// running maintenance, stopping that maintenance. If there are multiple threads
// running maintenance, they will all be stopped.
func (c *Contractor) managedInterruptContractMaintenance() {
	// Spin up a thread to grab the maintenance lock. Signal that the lock was
	// acquired after the lock is acquired.
	gotLock := make(chan struct{})
	go func() {
		c.maintenanceLock.Lock()
		close(gotLock)
		c.maintenanceLock.Unlock()
	}()

	// There may be multiple threads contending for the maintenance lock. Issue
	// interrupts repeatedly until we get a signal that the maintenance lock has
	// been acquired.
	for {
		select {
		case <-gotLock:
			return
		case c.interruptMaintenance <- struct{}{}:
		}
	}
}

// managedMarkContractsUtility checks every active contract in the contractor and
// figures out whether the contract is useful for uploading, and whether the
// contract should be renewed.
func (c *Contractor) managedMarkContractsUtility() error {
	// Pull a new set of hosts from the hostdb that could be used as a new set
	// to match the allowance. The lowest scoring host of these new hosts will
	// be used as a baseline for determining whether our existing contracts are
	// worthwhile.
	c.mu.RLock()
	hostCount := int(c.allowance.Hosts)
	period := c.allowance.Period
	c.mu.RUnlock()
	hosts, err := c.hdb.RandomHosts(hostCount+randomHostsBufferForScore, nil, nil)
	if err != nil {
		return err
	}

	// Find the minimum score that a host is allowed to have to be considered
	// good for upload.
	var minScore types.Currency
	if len(hosts) > 0 {
		sb, err := c.hdb.ScoreBreakdown(hosts[0])
		if err != nil {
			return err
		}
		lowestScore := sb.Score
		for i := 1; i < len(hosts); i++ {
			score, err := c.hdb.ScoreBreakdown(hosts[i])
			if err != nil {
				return err
			}
			if score.Score.Cmp(lowestScore) < 0 {
				lowestScore = score.Score
			}
		}
		// Set the minimum acceptable score to a factor of the lowest score.
		minScore = lowestScore.Div(scoreLeeway)
	}

	// Update utility fields for each contract.
	for _, contract := range c.staticContracts.ViewAll() {
		utility, err := func() (u modules.ContractUtility, err error) {
			u = contract.Utility

			// Start the contract in good standing if the utility isn't locked
			// but don't completely ignore the utility. A locked utility can
			// always get worse but not better.
			if !u.Locked {
				u.GoodForUpload = true
				u.GoodForRenew = true
			}

			host, exists := c.hdb.Host(contract.HostPublicKey)
			// Contract has no utility if the host is not in the database. Or is
			// filtered by the blacklist or whitelist.
			if !exists || host.Filtered {
				u.GoodForUpload = false
				u.GoodForRenew = false
				return u, nil
			}

			// Contract has no utility if the score is poor.
			sb, err := c.hdb.ScoreBreakdown(host)
			if err != nil {
				return u, err
			}
			if !minScore.IsZero() && sb.Score.Cmp(minScore) < 0 {
				u.GoodForUpload = false
				u.GoodForRenew = false
				return u, nil
			}

			// Contract has no utility if the host is offline.
			if isOffline(host) {
				u.GoodForUpload = false
				u.GoodForRenew = false
				return u, nil
			}

			// Contract should not be used for uploading if the time has come to
			// renew the contract.
			c.mu.RLock()
			blockHeight := c.blockHeight
			renewWindow := c.allowance.RenewWindow
			c.mu.RUnlock()
			if blockHeight+renewWindow >= contract.EndHeight {
				u.GoodForUpload = false
				return u, nil
			}

			// Contract should not be used for uploading if the contract does
			// not have enough money remaining to perform the upload.
			blockBytes := types.NewCurrency64(modules.SectorSize * uint64(period))
			sectorStoragePrice := host.StoragePrice.Mul(blockBytes)
			sectorUploadBandwidthPrice := host.UploadBandwidthPrice.Mul64(modules.SectorSize)
			sectorDownloadBandwidthPrice := host.DownloadBandwidthPrice.Mul64(modules.SectorSize)
			sectorBandwidthPrice := sectorUploadBandwidthPrice.Add(sectorDownloadBandwidthPrice)
			sectorPrice := sectorStoragePrice.Add(sectorBandwidthPrice)
			percentRemaining, _ := big.NewRat(0, 1).SetFrac(contract.RenterFunds.Big(), contract.TotalCost.Big()).Float64()
			if contract.RenterFunds.Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundUploadThreshold {
				u.GoodForUpload = false
				return u, nil
			}
			return u, nil
		}()
		if err != nil {
			return err
		}

		// Apply changes.
		err = c.managedUpdateContractUtility(contract.ID, utility)
		if err != nil {
			return err
		}
	}
	return nil
}

// managedNewContract negotiates an initial file contract with the specified
// host, saves it, and returns it.
func (c *Contractor) managedNewContract(host modules.HostDBEntry, contractFunding types.Currency, endHeight types.BlockHeight) (types.Currency, modules.RenterContract, error) {
	// reject hosts that are too expensive
	if host.StoragePrice.Cmp(maxStoragePrice) > 0 {
		return types.ZeroCurrency, modules.RenterContract{}, errTooExpensive
	}
	// Determine if host settings align with allowance period
	c.mu.Lock()
	if reflect.DeepEqual(c.allowance, modules.Allowance{}) {
		c.mu.Unlock()
		return types.ZeroCurrency, modules.RenterContract{}, errors.New("called managedNewContract but allowance wasn't set")
	}
	period := c.allowance.Period
	c.mu.Unlock()

	if host.MaxDuration < period {
		err := errors.New("unable to form contract with host due to insufficient MaxDuration of host")
		return types.ZeroCurrency, modules.RenterContract{}, err
	}
	// cap host.MaxCollateral
	if host.MaxCollateral.Cmp(maxCollateral) > 0 {
		host.MaxCollateral = maxCollateral
	}

	// get an address to use for negotiation
	uc, err := c.wallet.NextAddress()
	if err != nil {
		return types.ZeroCurrency, modules.RenterContract{}, err
	}

	// get the wallet seed.
	seed, _, err := c.wallet.PrimarySeed()
	if err != nil {
		return types.ZeroCurrency, modules.RenterContract{}, err
	}
	// derive the renter seed and wipe it once we are done with it.
	renterSeed := proto.DeriveRenterSeed(seed)
	defer fastrand.Read(renterSeed[:])

	// create contract params
	c.mu.RLock()
	params := proto.ContractParams{
		Allowance:     c.allowance,
		Host:          host,
		Funding:       contractFunding,
		StartHeight:   c.blockHeight,
		EndHeight:     endHeight,
		RefundAddress: uc.UnlockHash(),
		RenterSeed:    renterSeed.EphemeralRenterSeed(endHeight),
	}
	c.mu.RUnlock()

	// wipe the renter seed once we are done using it.
	defer fastrand.Read(params.RenterSeed[:])

	// create transaction builder and trigger contract formation.
	txnBuilder, err := c.wallet.StartTransaction()
	if err != nil {
		return types.ZeroCurrency, modules.RenterContract{}, err
	}
	contract, err := c.staticContracts.FormContract(params, txnBuilder, c.tpool, c.hdb, c.tg.StopChan())
	if err != nil {
		txnBuilder.Drop()
		return types.ZeroCurrency, modules.RenterContract{}, err
	}

	// Add a mapping from the contract's id to the public key of the host.
	c.mu.Lock()
	_, exists := c.pubKeysToContractID[contract.HostPublicKey.String()]
	if exists {
		c.mu.Unlock()
		txnBuilder.Drop()
		// We need to return a funding value because money was spent on this
		// host, even though the full process could not be completed.
		c.log.Println("WARN: Attempted to form a new contract with a host that we already have a contrat with.")
		return contractFunding, modules.RenterContract{}, fmt.Errorf("We already have a contract with host %v", contract.HostPublicKey)
	}
	c.pubKeysToContractID[contract.HostPublicKey.String()] = contract.ID
	c.mu.Unlock()

	contractValue := contract.RenterFunds
	c.log.Printf("Formed contract %v with %v for %v", contract.ID, host.NetAddress, contractValue.HumanString())
	return contractFunding, contract, nil
}

// managedPrunePubkeyMap will delete any pubkeys in the pubKeysToContractID map
// that no longer map to an active contract.
func (c *Contractor) managedPrunePubkeyMap() {
	allContracts := c.staticContracts.ViewAll()
	pks := make(map[string]struct{})
	for _, c := range allContracts {
		pks[c.HostPublicKey.String()] = struct{}{}
	}
	c.mu.Lock()
	for pk := range c.pubKeysToContractID {
		if _, exists := pks[pk]; !exists {
			delete(c.pubKeysToContractID, pk)
		}
	}
	c.mu.Unlock()
}

// managedPrunedRedundantAddressRange uses the hostdb to find hosts that
// violate the rules about address ranges and cancels them.
func (c *Contractor) managedPrunedRedundantAddressRange() {
	// Get all contracts which are not canceled.
	allContracts := c.staticContracts.ViewAll()
	var contracts []modules.RenterContract
	for _, contract := range allContracts {
		if contract.Utility.Locked && !contract.Utility.GoodForRenew && !contract.Utility.GoodForUpload {
			// contract is canceled
			continue
		}
		contracts = append(contracts, contract)
	}

	// Get all the public keys and map them to contract ids.
	pks := make([]types.SiaPublicKey, 0, len(allContracts))
	cids := make(map[string]types.FileContractID)
	for _, contract := range contracts {
		pks = append(pks, contract.HostPublicKey)
		cids[contract.HostPublicKey.String()] = contract.ID
	}

	// Let the hostdb filter out bad hosts and cancel contracts with those
	// hosts.
	badHosts := c.hdb.CheckForIPViolations(pks)
	for _, host := range badHosts {
		if err := c.managedCancelContract(cids[host.String()]); err != nil {
			c.log.Print("WARNING: Wasn't able to cancel contract in managedPrunedRedundantAddressRange", err)
		}
	}
}

// managedRenew negotiates a new contract for data already stored with a host.
// It returns the new contract. This is a blocking call that performs network
// I/O.
func (c *Contractor) managedRenew(sc *proto.SafeContract, contractFunding types.Currency, newEndHeight types.BlockHeight) (modules.RenterContract, error) {
	// For convenience
	contract := sc.Metadata()
	// Sanity check - should not be renewing a bad contract.
	utility, ok := c.managedContractUtility(contract.ID)
	if !ok || !utility.GoodForRenew {
		c.log.Critical(fmt.Sprintf("Renewing a contract that has been marked as !GoodForRenew %v/%v",
			ok, utility.GoodForRenew))
	}

	// Fetch the host associated with this contract.
	host, ok := c.hdb.Host(contract.HostPublicKey)
	c.mu.Lock()
	if reflect.DeepEqual(c.allowance, modules.Allowance{}) {
		c.mu.Unlock()
		return modules.RenterContract{}, errors.New("called managedRenew but allowance isn't set")
	}
	period := c.allowance.Period
	c.mu.Unlock()
	if !ok {
		return modules.RenterContract{}, errors.New("no record of that host")
	} else if host.Filtered {
		return modules.RenterContract{}, errors.New("host is blacklisted")
	} else if host.StoragePrice.Cmp(maxStoragePrice) > 0 {
		return modules.RenterContract{}, errTooExpensive
	} else if host.MaxDuration < period {
		return modules.RenterContract{}, errors.New("insufficient MaxDuration of host")
	}

	// cap host.MaxCollateral
	if host.MaxCollateral.Cmp(maxCollateral) > 0 {
		host.MaxCollateral = maxCollateral
	}

	// get an address to use for negotiation
	uc, err := c.wallet.NextAddress()
	if err != nil {
		return modules.RenterContract{}, err
	}

	// get the wallet seed
	seed, _, err := c.wallet.PrimarySeed()
	if err != nil {
		return modules.RenterContract{}, err
	}
	// derive the renter seed and wipe it after we are done with it.
	renterSeed := proto.DeriveRenterSeed(seed)
	defer fastrand.Read(renterSeed[:])

	// create contract params
	c.mu.RLock()
	params := proto.ContractParams{
		Allowance:     c.allowance,
		Host:          host,
		Funding:       contractFunding,
		StartHeight:   c.blockHeight,
		EndHeight:     newEndHeight,
		RefundAddress: uc.UnlockHash(),
		RenterSeed:    renterSeed.EphemeralRenterSeed(newEndHeight),
	}
	c.mu.RUnlock()

	// wipe the renter seed once we are done using it.
	defer fastrand.Read(params.RenterSeed[:])

	// execute negotiation protocol
	txnBuilder, err := c.wallet.StartTransaction()
	if err != nil {
		return modules.RenterContract{}, err
	}
	newContract, err := c.staticContracts.Renew(sc, params, txnBuilder, c.tpool, c.hdb, c.tg.StopChan())
	if err != nil {
		txnBuilder.Drop() // return unused outputs to wallet
		return modules.RenterContract{}, err
	}

	// Add a mapping from the contract's id to the public key of the host. This
	// will destroy the previous mapping from pubKey to contract id but other
	// modules are only interested in the most recent contract anyway.
	c.mu.Lock()
	c.pubKeysToContractID[newContract.HostPublicKey.String()] = newContract.ID
	c.mu.Unlock()

	return newContract, nil
}

// managedRenewContract will use the renew instructions to renew a contract,
// returning the amount of money that was put into the contract for renewal.
func (c *Contractor) managedRenewContract(renewInstructions fileContractRenewal, currentPeriod types.BlockHeight, allowance modules.Allowance, blockHeight, endHeight types.BlockHeight) (fundsSpent types.Currency, err error) {
	// Pull the variables out of the renewal.
	id := renewInstructions.id
	amount := renewInstructions.amount

	// Mark the contract as being renewed, and defer logic to unmark it
	// once renewing is complete.
	c.log.Debugln("Marking a contract for renew:", id)
	c.mu.Lock()
	c.renewing[id] = true
	c.mu.Unlock()
	defer func() {
		c.log.Debugln("Unmarking the contract for renew", id)
		c.mu.Lock()
		delete(c.renewing, id)
		c.mu.Unlock()
	}()

	// Wait for any active editors/downloaders/sessions to finish for this
	// contract, and then grab the latest revision.
	c.mu.RLock()
	e, eok := c.editors[id]
	d, dok := c.downloaders[id]
	s, sok := c.sessions[id]
	c.mu.RUnlock()
	if eok {
		c.log.Debugln("Waiting for editor invalidation")
		e.invalidate()
		c.log.Debugln("Got editor invalidation")
	}
	if dok {
		c.log.Debugln("Waiting for downloader invalidation")
		d.invalidate()
		c.log.Debugln("Got downloader invalidation")
	}
	if sok {
		c.log.Debugln("Waiting for session invalidation")
		s.invalidate()
		c.log.Debugln("Got session invalidation")
	}

	// Fetch the contract that we are renewing.
	c.log.Debugln("Acquiring contract from the contract set", id)
	oldContract, exists := c.staticContracts.Acquire(id)
	if !exists {
		c.log.Debugln("Contract does not seem to exist")
		return types.ZeroCurrency, errors.New("contract no longer exists")
	}
	// Return the contract if it's not useful for renewing.
	oldUtility, ok := c.managedContractUtility(id)
	if !ok || !oldUtility.GoodForRenew {
		c.log.Printf("Contract %v slated for renew is marked not good for renew: %v /%v",
			id, ok, oldUtility.GoodForRenew)
		c.staticContracts.Return(oldContract)
		return types.ZeroCurrency, errors.New("contract is marked not good for renew")
	}

	// Perform the actual renew. If the renew fails, return the
	// contract. If the renew fails we check how often it has failed
	// before. Once it has failed for a certain number of blocks in a
	// row and reached its second half of the renew window, we give up
	// on renewing it and set goodForRenew to false.
	c.log.Debugln("calling managedRenew on contract", id)
	newContract, errRenew := c.managedRenew(oldContract, amount, endHeight)
	c.log.Debugln("managedRenew has returned with error:", errRenew)
	if errRenew != nil {
		// Increment the number of failed renews for the contract if it
		// was the host's fault.
		if modules.IsHostsFault(errRenew) {
			c.mu.Lock()
			c.numFailedRenews[oldContract.Metadata().ID]++
			totalFailures := c.numFailedRenews[oldContract.Metadata().ID]
			c.mu.Unlock()
			c.log.Debugln("remote host determined to be at fault, tallying up failed renews", totalFailures, id)
		}

		// Check if contract has to be replaced.
		md := oldContract.Metadata()
		c.mu.RLock()
		numRenews, failedBefore := c.numFailedRenews[md.ID]
		c.mu.RUnlock()
		secondHalfOfWindow := blockHeight+allowance.RenewWindow/2 >= md.EndHeight
		replace := numRenews >= consecutiveRenewalsBeforeReplacement
		if failedBefore && secondHalfOfWindow && replace {
			oldUtility.GoodForRenew = false
			oldUtility.GoodForUpload = false
			oldUtility.Locked = true
			err := oldContract.UpdateUtility(oldUtility)
			if err != nil {
				c.log.Println("WARN: failed to mark contract as !goodForRenew:", err)
			}
			c.log.Printf("WARN: consistently failed to renew %v, marked as bad and locked: %v\n",
				oldContract.Metadata().HostPublicKey, errRenew)
			c.staticContracts.Return(oldContract)
			return types.ZeroCurrency, errors.AddContext(errRenew, "contract marked as bad for too many consecutive failed renew attempts")
		}

		// Seems like it doesn't have to be replaced yet. Log the
		// failure and number of renews that have failed so far.
		c.log.Printf("WARN: failed to renew contract %v [%v]: %v\n",
			oldContract.Metadata().HostPublicKey, numRenews, errRenew)
		c.staticContracts.Return(oldContract)
		return types.ZeroCurrency, errors.AddContext(errRenew, "contract renewal with host was unsuccessful")
	}
	c.log.Printf("Renewed contract %v\n", id)

	// Update the utility values for the new contract, and for the old
	// contract.
	newUtility := modules.ContractUtility{
		GoodForUpload: true,
		GoodForRenew:  true,
	}
	if err := c.managedUpdateContractUtility(newContract.ID, newUtility); err != nil {
		c.log.Println("Failed to update the contract utilities", err)
		c.staticContracts.Return(oldContract)
		return amount, nil // Error is not returned because the renew succeeded.
	}
	oldUtility.GoodForRenew = false
	oldUtility.GoodForUpload = false
	oldUtility.Locked = true
	if err := oldContract.UpdateUtility(oldUtility); err != nil {
		c.log.Println("Failed to update the contract utilities", err)
		c.staticContracts.Return(oldContract)
		return amount, nil // Error is not returned because the renew succeeded.
	}

	if c.staticDeps.Disrupt("InterruptContractSaveToDiskAfterDeletion") {
		c.staticContracts.Return(oldContract)
		return amount, errors.New("InterruptContractSaveToDiskAfterDeletion disrupt")
	}
	// Lock the contractor as we update it to use the new contract
	// instead of the old contract.
	c.mu.Lock()
	// Link Contracts
	c.renewedFrom[newContract.ID] = id
	c.renewedTo[id] = newContract.ID
	// Store the contract in the record of historic contracts.
	c.oldContracts[id] = oldContract.Metadata()
	// Save the contractor.
	err = c.saveSync()
	if err != nil {
		c.log.Println("Failed to save the contractor after creating a new contract.")
	}
	c.mu.Unlock()
	// Delete the old contract.
	c.staticContracts.Delete(oldContract)
	return amount, nil
}

// threadedContractMaintenance checks the set of contracts that the contractor
// has against the allownace, renewing any contracts that need to be renewed,
// dropping contracts which are no longer worthwhile, and adding contracts if
// there are not enough.
//
// Between each network call, the thread checks whether a maintenance interrupt
// signal is being sent. If so, maintenance returns, yielding to whatever thread
// issued the interrupt.
func (c *Contractor) threadedContractMaintenance() {
	err := c.tg.Add()
	if err != nil {
		return
	}
	defer c.tg.Done()
	c.log.Debugln("starting contract maintenance")

	// Only one instance of this thread should be running at a time. Under
	// normal conditions, fine to return early if another thread is already
	// doing maintenance. The next block will trigger another round. Under
	// testing, control is insufficient if the maintenance loop isn't guaranteed
	// to run.
	if build.Release == "testing" {
		c.maintenanceLock.Lock()
	} else if !c.maintenanceLock.TryLock() {
		c.log.Debugln("maintenance lock could not be obtained")
		return
	}
	defer c.maintenanceLock.Unlock()

	// Perform general cleanup of the contracts. This includes recovering lost
	// contracts, archiving contracts, and other cleanup work. This should all
	// happen before the rest of the maintenance.
	c.managedRecoverContracts()
	c.managedArchiveContracts()
	c.managedCheckForDuplicates()
	c.managedPrunePubkeyMap()
	c.managedPrunedRedundantAddressRange()
	err = c.managedMarkContractsUtility()
	if err != nil {
		c.log.Debugln("Unable to mark contract utilities:", err)
		return
	}

	// If there are no hosts requested by the allowance, there is no remaining
	// work.
	c.mu.RLock()
	wantedHosts := c.allowance.Hosts
	c.mu.RUnlock()
	if wantedHosts <= 0 {
		c.log.Debugln("Exiting contract maintenance because the number of desired hosts is <= zero.")
		return
	}

	// The rest of this function needs to know a few of the stateful variables
	// from the contractor, build those up under a lock so that the rest of the
	// function can execute without lock contention.
	c.mu.Lock()
	allowance := c.allowance
	blockHeight := c.blockHeight
	currentPeriod := c.currentPeriod
	endHeight := c.contractEndHeight()
	c.mu.Unlock()

	// Create the renewSet and refreshSet. Each is a list of contracts that need
	// to be renewed, paired with the amount of money to use in each renewal.
	//
	// The renewSet is specifically contracts which are being renewed because
	// they are about to expire. And the refreshSet is contracts that are being
	// renewed because they are out of money.
	//
	// The contractor will prioritize contracts in the renewSet over contracts
	// in the refreshSet. If the wallet does not have enough money, or if the
	// allowance does not have enough money, the contractor will prefer to save
	// data in the long term rather than renew a contract.
	var renewSet []fileContractRenewal
	var refreshSet []fileContractRenewal

	// Iterate through the contracts again, figuring out which contracts to
	// renew and how much extra funds to renew them with.
	for _, contract := range c.staticContracts.ViewAll() {
		c.log.Debugln("Examining a contract:", contract.HostPublicKey, contract.ID)
		// Skip any host that does not match our whitelist/blacklist filter
		// settings.
		host, _ := c.hdb.Host(contract.HostPublicKey)
		if host.Filtered {
			c.log.Debugln("Contract skipped because it is filtered")
			continue
		}

		// Skip any contracts which do not exist or are otherwise unworthy for
		// renewal.
		utility, ok := c.managedContractUtility(contract.ID)
		if !ok || !utility.GoodForRenew {
			c.log.Debugln("Contract skipped because it is not good for renew (utility.GoodForRenew, exists)", utility.GoodForRenew, ok)
			continue
		}

		// If the contract needs to be renewed because it is about to expire,
		// calculate a spending for the contract that is proportional to how
		// much money was spend on the contract throughout this billing cycle
		// (which is now ending).
		if blockHeight+allowance.RenewWindow >= contract.EndHeight {
			renewAmount, err := c.managedEstimateRenewFundingRequirements(contract, blockHeight, allowance)
			if err != nil {
				c.log.Debugln("Contract skipped because there was an error estimating renew funding requirements", renewAmount, err)
				continue
			}
			renewSet = append(renewSet, fileContractRenewal{
				id:     contract.ID,
				amount: renewAmount,
			})
			c.log.Debugln("Contract has been added to the renew set for being past the renew height")
			continue
		}

		// Check if the contract is empty. We define a contract as being empty
		// if less than 'minContractFundRenewalThreshold' funds are remaining
		// (3% at time of writing), or if there is less than 3 sectors worth of
		// storage+upload+download remaining.
		blockBytes := types.NewCurrency64(modules.SectorSize * uint64(allowance.Period))
		sectorStoragePrice := host.StoragePrice.Mul(blockBytes)
		sectorUploadBandwidthPrice := host.UploadBandwidthPrice.Mul64(modules.SectorSize)
		sectorDownloadBandwidthPrice := host.DownloadBandwidthPrice.Mul64(modules.SectorSize)
		sectorBandwidthPrice := sectorUploadBandwidthPrice.Add(sectorDownloadBandwidthPrice)
		sectorPrice := sectorStoragePrice.Add(sectorBandwidthPrice)
		percentRemaining, _ := big.NewRat(0, 1).SetFrac(contract.RenterFunds.Big(), contract.TotalCost.Big()).Float64()
		if contract.RenterFunds.Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundRenewalThreshold {
			// Renew the contract with double the amount of funds that the
			// contract had previously. The reason that we double the funding
			// instead of doing anything more clever is that we don't know what
			// the usage pattern has been. The spending could have all occurred
			// in one burst recently, and the user might need a contract that
			// has substantially more money in it.
			//
			// We double so that heavily used contracts can grow in funding
			// quickly without consuming too many transaction fees, however this
			// does mean that a larger percentage of funds get locked away from
			// the user in the event that the user stops uploading immediately
			// after the renew.
			refreshSet = append(refreshSet, fileContractRenewal{
				id:     contract.ID,
				amount: contract.TotalCost.Mul64(2),
			})
			c.log.Debugln("Contract identified as needing to be added to refresh set", contract.RenterFunds, sectorPrice.Mul64(3), percentRemaining, minContractFundRenewalThreshold)
		} else {
			c.log.Debugln("Contract did not get added to the refresh set", contract.RenterFunds, sectorPrice.Mul64(3), percentRemaining, minContractFundRenewalThreshold)
		}
	}
	if len(renewSet) != 0 || len(refreshSet) != 0 {
		c.log.Printf("renewing %v contracts and refreshing %v contracts", len(renewSet), len(refreshSet))
	}

	// Update the failed renew map so that it only contains contracts which we
	// are currently trying to renew or refresh. The failed renew map is a map
	// that we use to track how many times consecutively we failed to renew a
	// contract with a host, so that we know if we need to abandon that host.
	c.mu.Lock()
	newFirstFailedRenew := make(map[types.FileContractID]types.BlockHeight)
	for _, r := range renewSet {
		if _, exists := c.numFailedRenews[r.id]; exists {
			newFirstFailedRenew[r.id] = c.numFailedRenews[r.id]
		}
	}
	for _, r := range refreshSet {
		if _, exists := c.numFailedRenews[r.id]; exists {
			newFirstFailedRenew[r.id] = c.numFailedRenews[r.id]
		}
	}
	c.numFailedRenews = newFirstFailedRenew
	c.mu.Unlock()

	// Depend on the PeriodSpending function to get a breakdown of spending in
	// the contractor. Then use that to determine how many funds remain
	// available in the allowance for renewals.
	spending := c.PeriodSpending()
	var fundsRemaining types.Currency
	// Check for an underflow. This can happen if the user reduced their
	// allowance at some point to less than what we've already spent.
	if spending.TotalAllocated.Cmp(allowance.Funds) < 0 {
		fundsRemaining = allowance.Funds.Sub(spending.TotalAllocated)
	}
	c.log.Debugln("The allowance has this many remaning funds:", fundsRemaining)

	// Go through the contracts we've assembled for renewal. Any contracts that
	// need to be renewed because they are expiring (renewSet) get priority over
	// contracts that need to be renewed because they have exhausted their funds
	// (refreshSet). If there is not enough money available, the more expensive
	// contracts will be skipped.
	for _, renewal := range renewSet {
		// TODO: Check if the wallet is unlocked here. If the wallet is locked,
		// exit here.

		c.log.Println("Attempting to perform a renewal:", renewal.id)
		// Skip this renewal if we don't have enough funds remaining.
		if renewal.amount.Cmp(fundsRemaining) > 0 {
			c.log.Debugln("Skipping renewal because there are not enough funds remaining in the allowance", renewal.id, renewal.amount, fundsRemaining)
			continue
		}

		// Renew one contract. The error is ignored because the renew function
		// already will have logged the error, and in the event of an error,
		// 'fundsSpent' will return '0'.
		fundsSpent, err := c.managedRenewContract(renewal, currentPeriod, allowance, blockHeight, endHeight)
		if err != nil {
			c.log.Println("Error renewing a contract", renewal.id, err)
		} else {
			c.log.Println("Renewal completed without error")
		}
		fundsRemaining = fundsRemaining.Sub(fundsSpent)

		// Return here if an interrupt or kill signal has been sent.
		select {
		case <-c.tg.StopChan():
			c.log.Println("returning because the renter was stopped")
			return
		case <-c.interruptMaintenance:
			c.log.Println("returning because maintenance was interrupted")
			return
		default:
		}
	}
	for _, renewal := range refreshSet {
		// TODO: Check if the wallet is unlocked here. If the wallet is locked,
		// exit here.

		// Skip this renewal if we don't have enough funds remaining.
		c.log.Debugln("Attempting to perform a contract refresh:", renewal.id)
		if renewal.amount.Cmp(fundsRemaining) > 0 {
			c.log.Println("skipping refresh because there are not enough funds remaining in the allowance", renewal.amount, fundsRemaining)
			continue
		}

		// Renew one contract. The error is ignored because the renew function
		// already will have logged the error, and in the event of an error,
		// 'fundsSpent' will return '0'.
		fundsSpent, err := c.managedRenewContract(renewal, currentPeriod, allowance, blockHeight, endHeight)
		if err != nil {
			c.log.Println("Error refreshing a contract", renewal.id, err)
		}
		fundsRemaining = fundsRemaining.Sub(fundsSpent)

		// Return here if an interrupt or kill signal has been sent.
		select {
		case <-c.tg.StopChan():
			c.log.Println("returning because the renter was stopped")
			return
		case <-c.interruptMaintenance:
			c.log.Println("returning because maintenance was interrupted")
			return
		default:
		}
	}

	// Count the number of contracts which are good for uploading, and then make
	// more as needed to fill the gap.
	uploadContracts := 0
	for _, id := range c.staticContracts.IDs() {
		if cu, ok := c.managedContractUtility(id); ok && cu.GoodForUpload {
			uploadContracts++
		}
	}
	c.mu.RLock()
	neededContracts := int(c.allowance.Hosts) - uploadContracts
	c.mu.RUnlock()
	if neededContracts <= 0 {
		c.log.Debugln("do not seem to need more contracts")
		return
	}
	c.log.Println("need more contracts:", neededContracts)

	// Assemble two exclusion lists. The first one includes all hosts that we
	// already have contracts with and the second one includes all hosts we
	// have active contracts with. Then select a new batch of hosts to attempt
	// contract formation with.
	allContracts := c.staticContracts.ViewAll()
	c.mu.RLock()
	var blacklist []types.SiaPublicKey
	var addressBlacklist []types.SiaPublicKey
	for _, contract := range allContracts {
		blacklist = append(blacklist, contract.HostPublicKey)
		if !contract.Utility.Locked || contract.Utility.GoodForRenew || contract.Utility.GoodForUpload {
			addressBlacklist = append(addressBlacklist, contract.HostPublicKey)
		}
	}
	// Add the hosts we have recoverable contracts with to the blacklist to
	// avoid losing existing data by forming a new/empty contract.
	for _, contract := range c.recoverableContracts {
		blacklist = append(blacklist, contract.HostPublicKey)
	}

	initialContractFunds := c.allowance.Funds.Div64(c.allowance.Hosts).Div64(3)
	c.mu.RUnlock()
	hosts, err := c.hdb.RandomHosts(neededContracts*2+randomHostsBufferForScore, blacklist, addressBlacklist)
	if err != nil {
		c.log.Println("WARN: not forming new contracts:", err)
		return
	}

	// Form contracts with the hosts one at a time, until we have enough
	// contracts.
	for _, host := range hosts {
		// TODO: Check if the wallet is unlocked here. If the wallet is locked,
		// exit here.

		// Determine if we have enough money to form a new contract.
		if fundsRemaining.Cmp(initialContractFunds) < 0 {
			c.log.Println("WARN: need to form new contracts, but unable to because of a low allowance")
			break
		}

		// If we are using a custom resolver we need to replace the domain name
		// with 127.0.0.1 to be able to form contracts.
		if c.staticDeps.Disrupt("customResolver") {
			port := host.NetAddress.Port()
			host.NetAddress = modules.NetAddress(fmt.Sprintf("127.0.0.1:%s", port))
		}

		// Attempt forming a contract with this host.
		fundsSpent, newContract, err := c.managedNewContract(host, initialContractFunds, endHeight)
		if err != nil {
			c.log.Printf("Attempted to form a contract with %v, but negotiation failed: %v\n", host.NetAddress, err)
			continue
		}
		fundsRemaining = fundsRemaining.Sub(fundsSpent)

		// Add this contract to the contractor and save.
		err = c.managedUpdateContractUtility(newContract.ID, modules.ContractUtility{
			GoodForUpload: true,
			GoodForRenew:  true,
		})
		if err != nil {
			c.log.Println("Failed to update the contract utilities", err)
			return
		}
		c.mu.Lock()
		err = c.saveSync()
		c.mu.Unlock()
		if err != nil {
			c.log.Println("Unable to save the contractor:", err)
		}

		// Quit the loop if we've replaced all needed contracts.
		neededContracts--
		if neededContracts <= 0 {
			break
		}

		// Soft sleep before making the next contract.
		select {
		case <-c.tg.StopChan():
			return
		case <-c.interruptMaintenance:
			return
		default:
		}
	}
}

// managedUpdateContractUtility is a helper function that acquires a contract, updates
// its ContractUtility and returns the contract again.
func (c *Contractor) managedUpdateContractUtility(id types.FileContractID, utility modules.ContractUtility) error {
	safeContract, ok := c.staticContracts.Acquire(id)
	if !ok {
		return errors.New("failed to acquire contract for update")
	}
	defer c.staticContracts.Return(safeContract)
	return safeContract.UpdateUtility(utility)
}

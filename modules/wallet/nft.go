package wallet

import (
	"gitlab.com/NebulousLabs/errors"
	"go.sia.tech/siad/build"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

/// Contains chain-of-custody NFT functionality for
/// all primary wallet operations
/// Author: Ian McJohn

// allow room for significant amounts of arbitrary data
// in NFT transactions
const estimatedNFTTransactionSize = estimatedTransactionSize * 2.0

// Random valid address to use for NFT Lockup
// TODO: Switch to anyone-can-spend outputs

func signAndSend(w *Wallet, txnBuilder *(modules.TransactionBuilder)) (txns []types.Transaction, err error) {
	txnSet, err := (*txnBuilder).Sign(true)
	if err != nil {
		w.log.Println("Attempt to send coins has failed - failed to sign transaction:", err)
		return nil, build.ExtendErr("unable to sign transaction", err)
	}
	if w.deps.Disrupt("SendSiacoinsInterrupted") {
		return nil, errors.New("failed to accept transaction set (SendSiacoinsInterrupted)")
	}
	err = w.tpool.AcceptTransactionSet(txnSet)
	if err != nil {
		w.log.Println("Attempt to send coins has failed - transaction pool rejected transaction:", err)
		return nil, build.ExtendErr("unable to get transaction accepted", err)
	}
	for _, txn := range txnSet {
		w.log.Println("\t", txn.ID())
	}
	return txnSet, nil
}

func preNFTWalletSetup(w *Wallet) (txns []types.Transaction, err error) {
	if err := w.tg.Add(); err != nil {
		return nil, err
	}
	defer w.tg.Done()

	// Check if consensus is synced
	if !w.cs.Synced() || w.deps.Disrupt("UnsyncedConsensus") {
		return nil, errors.New("cannot send siacoin until fully synced")
	}

	w.mu.RLock()
	unlocked := w.unlocked
	w.mu.RUnlock()
	if !unlocked {
		w.log.Println("Attempt to send coins has failed - wallet is locked")
		return nil, modules.ErrLockedWallet
	}

	return nil, nil
}

func (w *Wallet) MintNFT(nft types.NftCustody, dest types.UnlockHash) (txns []types.Transaction, err error) {
	// Add to threadgroup, check locks
	_, err = preNFTWalletSetup(w)
	if err != nil {
		return nil, err // setup failed, pass the error on
	}

	// Create outputs for lockup pool, host pool, and colored-coin custody
	lockupOutput := types.SiacoinOutput{
		UnlockHash: types.NFTLockupUnlockConditions.UnlockHash(),
		Value:      types.NFTLockupAmount,
	}
	storagePoolOutput := types.SiacoinOutput{
		UnlockHash: types.NFTStoragePoolUnlockConditions.UnlockHash(),
		Value:      types.NFTLockupAmount,
	}
	NFTMintingOutput := types.SiacoinOutput{
		UnlockHash: dest,
		Value:      types.OneBaseUnit, // 1 tNFT sent to new address for minting
	}

	// Assemble transaction and fund
	_, fee := w.tpool.FeeEstimation()
	fee = fee.Mul64(estimatedNFTTransactionSize)
	totalCost := types.NFTHostAmount.Add(types.NFTLockupAmount).Add(types.OneBaseUnit).Add(fee)
	txnBuilder, err := w.StartTransaction()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			txnBuilder.Drop()
		}
	}()
	err = txnBuilder.FundSiacoins(totalCost)
	if err != nil {
		w.log.Println("Attempt to send coins has failed - failed to fund transaction:", err)
		return nil, build.ExtendErr("unable to fund transaction", err)
	}
	txnBuilder.AddMinerFee(fee)

	// Add Arbitrary Data specifier to prove NFT Minting Transaction for validators
	arbitraryData := types.PrefixNFTCustody[:]
	merkleRoot := []byte(nft.FileMerkleRoot.String())
	arbitraryData = append(arbitraryData, types.NFTMintTag...)
	arbitraryData = append(arbitraryData, merkleRoot...)
	txnBuilder.AddArbitraryData(arbitraryData)

	// Include outputs in transaction and send
	txnBuilder.AddSiacoinOutput(lockupOutput)
	txnBuilder.AddSiacoinOutput(storagePoolOutput)
	txnBuilder.AddSiacoinOutput(NFTMintingOutput)

	w.log.Println("Submitting an NFT Minting transaction for nft", nft.FileMerkleRoot, "with fees", fee.HumanString())
	return signAndSend(w, &txnBuilder)
}

func (w *Wallet) TransferNFT(nft types.NftCustody, dest types.UnlockHash) (txns []types.Transaction, err error) {
	// Add to threadgroup, check locks
	_, err = preNFTWalletSetup(w)
	if err != nil {
		return nil, err // setup failed, pass the error on
	}

	// Create outputs for transfer fees into host pool, and colored-coin custody
	storagePoolOutput := types.SiacoinOutput{
		UnlockHash: types.NFTStoragePoolUnlockConditions.UnlockHash(),
		Value:      types.NFTTransferCost,
	}
	NFTTransferOutput := types.SiacoinOutput{
		UnlockHash: dest,
		Value:      types.OneBaseUnit, // 1 tNFT sent to new address for transfer
	}

	// Assemble transaction and fund
	_, fee := w.tpool.FeeEstimation()
	fee = fee.Mul64(estimatedNFTTransactionSize)
	totalCost := types.NFTTransferCost.Add(fee)
	txnBuilder, err := w.StartTransaction()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			txnBuilder.Drop()
		}
	}()
	err = txnBuilder.FundSiacoins(totalCost)
	if err != nil {
		w.log.Println("Attempt to send coins has failed - failed to fund transaction:", err)
		return nil, build.ExtendErr("unable to fund transaction", err)
	}
	txnBuilder.AddMinerFee(fee)

	// Locate NFT output from previous chain-of-custody
	goalOutput, err := w.cs.ViewNFTCustody(nft)
	if err != nil {
		w.log.Println("Attempt to send NFT has failed - Could not locate NFT output for transfer")
		return nil, build.ExtendErr("unable to locate NFT output for transfer", err)
	}
	var goal_scoid types.SiacoinOutputID
	var goal_sco types.SiacoinOutput
	var found bool = false
	err = dbForEachSiacoinOutput(w.dbTx, func(scoid types.SiacoinOutputID, sco types.SiacoinOutput) {
		if sco.Value.Equals(goalOutput.Value) && sco.UnlockHash == goalOutput.UnlockHash {
			// Not guaranteed to be the same output that was used to transfer the NFT to this address
			// but as far as I know that shouldn't cause any problems? Haven't yet found a use-case
			// where it needs to be the same one. If it does we can start recording output ids in applytransaction
			goal_scoid = scoid
			goal_sco = sco
			found = true
		}
	})
	if err != nil || !found {
		w.log.Println("Attempt to locate NFT chain-of-custody has failed, perhaps sending an NFT that is not ours?")
		return nil, build.ExtendErr("unable to locate NFT within our wallet", err)
	}

	// Transform into input
	sci := types.SiacoinInput{
		ParentID:         goal_scoid,
		UnlockConditions: w.keys[goal_sco.UnlockHash].UnlockConditions,
	}
	txnBuilder.AddAndSignSiacoinInput(sci)

	// Add Arbitrary Data specifier to prove NFT Minting Transaction for validators
	arbitraryData := types.PrefixNFTCustody[:]
	merkleRoot := []byte(nft.FileMerkleRoot.String())
	arbitraryData = append(arbitraryData, types.NFTTransferTag...)
	arbitraryData = append(arbitraryData, merkleRoot...)
	txnBuilder.AddArbitraryData(arbitraryData)

	// Include outputs in transaction and send
	txnBuilder.AddSiacoinOutput(storagePoolOutput)
	txnBuilder.AddSiacoinOutput(NFTTransferOutput)
	w.log.Println("Submitting an NFT Transfer transaction for nft", nft.FileMerkleRoot, "with fees", fee.HumanString(), "IDs:")
	return signAndSend(w, &txnBuilder)
}

// Return all NFTs owned by this wallet as ownership stats
func (w *Wallet) ScanAllNFTS() []types.NftOwnershipStats {
	if err := w.tg.Add(); err != nil {
		return nil
	}
	defer w.tg.Done()

	var ret []types.NftOwnershipStats
	for key := range w.keys {
		for _, nft := range w.cs.FindNFTsForAddress(key) {
			var custody types.NftOwnershipStats
			custody.Nft = nft
			custody.Owner = key
			ret = append(ret, custody)
		}
	}
	return ret
}

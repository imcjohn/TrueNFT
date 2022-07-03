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

func (w *Wallet) MintNFT(nft types.NftCustody, dest types.UnlockHash) (txns []types.Transaction, err error) {
	// Load lockup condition structs
	NFTLockupUnlockConditions, NFTStoragePoolUnlockConditions := types.NFTPoolUnlockConditions()

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

	// Create outputs for lockup pool, host pool, and colored-coin custody
	lockupOutput := types.SiacoinOutput{
		UnlockHash: NFTLockupUnlockConditions.UnlockHash(),
		Value:      types.NFTLockupAmount,
	}
	storagePoolOutput := types.SiacoinOutput{
		UnlockHash: NFTStoragePoolUnlockConditions.UnlockHash(),
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
	arbitraryData := modules.PrefixNFTCustody[:]
	merkleRoot := []byte(nft.MerkleRoot.String())
	arbitraryData = append(arbitraryData, types.NFTMintTag...)
	arbitraryData = append(arbitraryData, merkleRoot...)
	txnBuilder.AddArbitraryData(arbitraryData)

	// Include outputs in transaction and send
	txnBuilder.AddSiacoinOutput(lockupOutput)
	txnBuilder.AddSiacoinOutput(storagePoolOutput)
	txnBuilder.AddSiacoinOutput(NFTMintingOutput)
	txnSet, err := txnBuilder.Sign(true)
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
	w.log.Println("Submitted an NFT Minting transaction for nft", nft.MerkleRoot, "with fees", fee.HumanString(), "IDs:")
	for _, txn := range txnSet {
		w.log.Println("\t", txn.ID())
	}
	return txnSet, nil
}

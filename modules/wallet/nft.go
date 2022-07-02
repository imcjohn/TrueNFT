package wallet

import (
	"math/big"

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

// Constants
func CurrencyFromConst(amount string) types.Currency {
	hastings, _ := types.ParseCurrency(amount)
	i, _ := new(big.Int).SetString(hastings, 10)
	c := types.NewCurrency(i)
	return c
}

// Network-specific costs
var (
	NFTMintCost     = CurrencyFromConst("5000SC")
	NFTLockupAmount = CurrencyFromConst("2500SC")
	NFTHostAmount   = CurrencyFromConst("2500SC")
	NFTTransferCost = CurrencyFromConst("500SC")
)

// Random valid address to use for NFT Lockup
// TODO: Switch to anyone-can-spend outputs
var NFTLockupAddress = types.MustParseAddress("4b339bd24e1e9f9688e259a703e523f26c3093f0b720ab247b1f5a82bf17a0cffef96354768b")
var NFTStoragePoolAddress = types.MustParseAddress("db5867e4a59232e5025fb01d960342128e496e3ae3e2c5c56a547f36000cee15bc54af9ee049")

func (w *Wallet) MintNFT(nft types.NftCustody, dest types.UnlockHash) (txns []types.Transaction, err error) {
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
		UnlockHash: NFTLockupAddress,
		Value:      NFTLockupAmount,
	}
	storagePoolOutput := types.SiacoinOutput{
		UnlockHash: NFTStoragePoolAddress,
		Value:      NFTLockupAmount,
	}
	NFTMintingOutput := types.SiacoinOutput{
		UnlockHash: dest,
		Value:      types.OneBaseUnit, // 1 tNFT sent to new address for minting
	}

	// Assemble transaction and fund
	_, fee := w.tpool.FeeEstimation()
	fee = fee.Mul64(estimatedNFTTransactionSize)
	totalCost := NFTHostAmount.Add(NFTLockupAmount).Add(types.OneBaseUnit).Add(fee)
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

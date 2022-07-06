package types

import (
	"encoding/hex"
	"math/big"

	"go.sia.tech/siad/crypto"
)

/// Contains core NFT types for internal representation of on-chain assets
/// Author: Ian McJohn

// Helper Functions
func CurrencyFromConst(amount string) Currency {
	hastings, _ := ParseCurrency(amount)
	i, _ := new(big.Int).SetString(hastings, 10)
	c := NewCurrency(i)
	return c
}

// Useful constants
var (
	NFTTagLen               = 2
	NFTMintTag              = []byte{'M', 'N'}
	NFTTransferTag          = []byte{'T', 'R'}
	NFTWithoutCustody       = SiacoinOutput{}
	LiquidatedNFTUnlockHash = UnlockHash{'L', 'Q'}
	// Network-specific costs
	NFTMintCost     = CurrencyFromConst("5000SC")
	NFTLockupAmount = CurrencyFromConst("2500SC")
	NFTHostAmount   = CurrencyFromConst("2500SC")
	NFTTransferCost = CurrencyFromConst("500SC")
	// PrefixNFTCustody means that this transaction is specially marked
	// as an NFT chain-of-custody transfer, and thus uses the arbitrary
	// data field
	PrefixNFTCustody = NewSpecifier("NFT")
)

// Discerning functions for filtering NFT transactions
func IsNFTTransaction(t Transaction) bool {
	// Don't run on non-nft transactions
	var prefix Specifier
	if len(t.ArbitraryData) < 1 {
		return false
	}
	nftTag := t.ArbitraryData[0]
	copy(prefix[:], nftTag)
	return prefix == PrefixNFTCustody
}

func IsNFTMintTransaction(t Transaction) bool {
	if !IsNFTTransaction(t) {
		return false
	}
	idx := SpecifierLen
	b1 := t.ArbitraryData[0][idx]
	b2 := t.ArbitraryData[0][idx+1]
	return b1 == NFTMintTag[0] && b2 == NFTMintTag[1]
}

func IsNFTTransferTransaction(t Transaction) bool {
	if !IsNFTTransaction(t) {
		return false
	}
	idx := SpecifierLen
	b1 := t.ArbitraryData[0][idx]
	b2 := t.ArbitraryData[0][idx+1]
	return b1 == NFTTransferTag[0] && b2 == NFTTransferTag[1]
}

// Remove NFT Information from arbitrary data section of transaction
func ExtractNFTFromTransaction(t Transaction) (NftCustody, SiacoinOutput) {
	NFTLockupUnlockConditions, NFTStoragePoolUnlockConditions := NFTPoolUnlockConditions()
	// First extract merkle root
	startIndex := SpecifierLen + NFTTagLen
	var ret NftCustody
	var owner SiacoinOutput
	var merkleRoot []byte = t.ArbitraryData[0][startIndex:]
	ret.MerkleRoot.LoadString(string(merkleRoot))
	// Then extract current owner
	// TODO: Modify to return dead value for liquidates
	for _, out := range t.SiacoinOutputs {
		h := out.UnlockHash
		if h != NFTLockupUnlockConditions.UnlockHash() && h != NFTStoragePoolUnlockConditions.UnlockHash() {
			owner = out // Valid NFT Transactions only have one non-payoff output
			break
		}
	}
	return ret, owner
}

// Function to create the unlock conditions for
// the two NFT storage pools
func NFTPoolUnlockConditions() (UnlockConditions, UnlockConditions) {
	// Lockup Conditions
	lockupPkey, _ := hex.DecodeString("4d652d8ce36facbf0c194a7533a7a2ee7c9c9e364af45e65cf7433e5b8496696")
	storagePoolPkey, _ := hex.DecodeString("171b3650f74b718fc003828e3e33a6b525f055db049bdefdc3122baba3e016e0")
	NFTLockupUnlockConditions := UnlockConditions{
		Timelock:           0,
		SignaturesRequired: 0,
		PublicKeys: []SiaPublicKey{{
			Algorithm: SignatureEd25519,
			Key:       lockupPkey,
		}},
	}
	NFTStoragePoolUnlockConditions := UnlockConditions{
		Timelock:           0,
		SignaturesRequired: 0,
		PublicKeys: []SiaPublicKey{{
			Algorithm: SignatureEd25519,
			Key:       storagePoolPkey,
		}},
	}

	return NFTLockupUnlockConditions, NFTStoragePoolUnlockConditions
}

// Core NFT Types
type (
	NftCustody struct {
		// merkle root corresponding to hash of NFT's data
		// used as unique identifier for NFT throughout codebase
		// ideally set this to a more useful/constrained type in the future
		MerkleRoot crypto.Hash
	}
	NftOwnershipStats struct {
		Nft   NftCustody `json:"nftroots"`
		Owner UnlockHash `json:"nftowner"`
	}
)

package types

import "go.sia.tech/siad/crypto"

/// Contains core NFT types for internal representation of on-chain assets
/// Author: Ian McJohn

// Types

type (
	NftCustody struct {
		// merkle root corresponding to hash of NFT's data
		// used as unique identifier for NFT throughout codebase
		// ideally set this to a more useful/constrained type in the future
		MerkleRoot crypto.Hash
	}
)

var (
	NFTMintTag = []byte{'M', 'N'}
)

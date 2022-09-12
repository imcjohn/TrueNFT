package types

type (
	// A bucketed pool acts as a container type
	// for the key corresponding to a pool's bucket
	// in the consensus database
	BucketedPool struct {
		NamedBucket []byte
	}
)

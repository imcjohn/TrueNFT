package siafile

import (
	"bytes"
	"fmt"
	"io"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/errors"
)

// RSSubCode is a Reed-Solomon encoder/decoder. It implements the
// modules.ErasureCoder interface in a way that every crypto.SegmentSize bytes
// of encoded data can be recovered separately.
type RSSubCode struct {
	staticSegmentSize uint64
	RSCode
}

// Encode splits data into equal-length pieces, some containing the original
// data and some containing parity data.
func (rs *RSSubCode) Encode(data []byte) ([][]byte, error) {
	pieces, err := rs.enc.Split(data)
	if err != nil {
		return nil, err
	}
	return rs.EncodeShards(pieces)
}

// EncodeShards encodes data in a way that every segmentSize bytes of the
// encoded data can be decoded independently.
func (rs *RSSubCode) EncodeShards(pieces [][]byte) ([][]byte, error) {
	// Check that there are enough pieces.
	if len(pieces) != rs.MinPieces() {
		return nil, fmt.Errorf("not enough segments expected %v but was %v",
			rs.MinPieces(), len(pieces))
	}
	// Since all the pieces should have the same length, get the pieceSize from
	// the first one.
	pieceSize := uint64(len(pieces[0]))
	segmentSize := rs.staticSegmentSize
	// pieceSize must be divisible by segmentSize
	if pieceSize%segmentSize != 0 {
		return nil, errors.New("pieceSize not divisible by segmentSize")
	}
	// Each piece should've pieceSize bytes.
	for _, piece := range pieces {
		if uint64(len(piece)) != pieceSize {
			return nil, fmt.Errorf("pieces don't have right size expected %v but was %v",
				pieceSize, len(piece))
		}
	}
	// Convert pieces.
	tmpPieces := make([][]byte, len(pieces))
	i := 0
	for _, piece := range pieces {
		for buf := bytes.NewBuffer(piece); buf.Len() > 0; {
			segment := buf.Next(int(segmentSize))
			pieceIndex := i % len(pieces)
			segmentIndex := i / len(pieces)
			if tmpPieces[pieceIndex] == nil {
				tmpPieces[pieceIndex] = make([]byte, pieceSize)
			}
			copy(tmpPieces[pieceIndex][uint64(segmentIndex)*segmentSize:][:segmentSize], segment)
			i++
		}
	}
	pieces = tmpPieces
	// Add the parity shards to pieces.
	for len(pieces) < rs.NumPieces() {
		pieces = append(pieces, make([]byte, pieceSize))
	}
	// Convert the pieces to segments.
	segments := make([][][]byte, pieceSize/segmentSize)
	for pieceIndex, piece := range pieces {
		for segmentIndex := uint64(0); segmentIndex < pieceSize/segmentSize; segmentIndex++ {
			// Allocate space for segments as needed.
			if segments[segmentIndex] == nil {
				segments[segmentIndex] = make([][]byte, rs.NumPieces())
			}
			segment := piece[segmentIndex*segmentSize:][:segmentSize]
			segments[segmentIndex][pieceIndex] = segment
		}
	}
	// Encode the segments.
	for i := range segments {
		encodedSegment, err := rs.RSCode.EncodeShards(segments[i])
		if err != nil {
			return nil, err
		}
		segments[i] = encodedSegment
	}
	return pieces, nil
}

// Recover accepts encoded pieces and decodes the segment at
// segmentIndex. The size of the decoded data is segmentSize * dataPieces.
func (rs *RSSubCode) Recover(pieces [][]byte, n uint64, w io.Writer) error {
	// Check the length of pieces.
	if len(pieces) != rs.NumPieces() {
		return fmt.Errorf("expected pieces to have len %v but was %v",
			rs.NumPieces(), len(pieces))
	}
	// Since all the pieces should have the same length, get the pieceSize from
	// the first piece that was set.
	var pieceSize uint64
	for _, piece := range pieces {
		if uint64(len(piece)) > pieceSize {
			pieceSize = uint64(len(piece))
			break
		}
	}
	segmentSize := rs.staticSegmentSize

	// pieceSize must be divisible by segmentSize
	if pieceSize%segmentSize != 0 {
		return errors.New("pieceSize not divisible by segmentSize")
	}

	// Extract the segment from the pieces.
	segment := make([][]byte, uint64(rs.NumPieces()))
	decodedSegmentSize := segmentSize * uint64(rs.MinPieces())
	for off := uint64(0); off < pieceSize && n > 0; off += segmentSize {
		for i, piece := range pieces {
			if uint64(len(piece)) > off {
				segment[i] = piece[off : off+segmentSize]
			} else {
				segment[i] = nil
			}
		}
		// Reconstruct the segment.
		if n < decodedSegmentSize {
			decodedSegmentSize = n
		}
		if err := rs.RSCode.Recover(segment, decodedSegmentSize, w); err != nil {
			return err
		}
		n -= decodedSegmentSize
	}
	return nil
}

// Type returns the erasure coders type identifier.
func (rs *RSSubCode) Type() modules.ErasureCoderType {
	return ecReedSolomonSubShards
}

// ExtractSegment is a convenience method that extracts the data of the segment
// at segmentIndex from pieces.
func ExtractSegment(pieces [][]byte, segmentIndex int) [][]byte {
	segment := make([][]byte, len(pieces))
	off := segmentIndex * crypto.SegmentSize
	for i := 0; i < len(pieces); i++ {
		if len(pieces[i]) > 0 {
			segment[i] = pieces[i][off : off+crypto.SegmentSize]
		}
	}
	return segment
}

// NewRSSubCode creates a new Reed-Solomon encoder/decoder using the supplied
// parameters.
func NewRSSubCode(nData, nParity int) (modules.ErasureCoder, error) {
	rs, err := newRSCode(nData, nParity)
	if err != nil {
		return nil, err
	}
	return &RSSubCode{
		crypto.SegmentSize,
		*rs,
	}, nil
}

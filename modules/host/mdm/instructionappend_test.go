package mdm

import (
	"bytes"
	"context"
	"testing"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/fastrand"
)

// newAppendProgram is a convenience method which prepares the instructions
// and the program data for a program that executes a single
// AppendInstruction.
func newAppendProgram(sectorData []byte, merkleProof bool) ([]modules.Instruction, []byte) {
	instructions := []modules.Instruction{
		NewAppendInstruction(0, merkleProof),
	}
	return instructions, sectorData
}

// TestInstructionAppend tests executing a program with a single
// AppendInstruction.
func TestInstructionAppend(t *testing.T) {
	host := newTestHost()
	mdm := New(host)
	defer mdm.Stop()

	// Create a program to read a full sector from the host.
	appendData := fastrand.Bytes(int(modules.SectorSize))
	appendDataRoot := crypto.MerkleRoot(appendData)
	instructions, programData := newAppendProgram(appendData, true)
	dataLen := uint64(len(programData))
	// Execute it.
	ics := uint64(0)     // initial contract size is 0 sectors.
	imr := crypto.Hash{} // initial merkle root is empty.
	so := newTestStorageObligation(true)
	pt := newTestPriceTable()
	finalize, outputs, err := mdm.ExecuteProgram(context.Background(), pt, instructions, InitCost(pt, dataLen).Add(WriteCost(pt, modules.SectorSize)), so, ics, imr, dataLen, bytes.NewReader(programData))
	if err != nil {
		t.Fatal(err)
	}
	// Execute program and count results.
	numOutputs := 0
	for output := range outputs {
		if err := output.Error; err != nil {
			t.Fatal(err)
		}
		if output.NewSize != ics+modules.SectorSize {
			t.Fatalf("expected contract size should increase by a sector size: %v != %v", ics+modules.SectorSize, output.NewSize)
		}
		if output.NewMerkleRoot != crypto.MerkleRoot(appendData) {
			t.Fatalf("expected merkle root to be root of appended sector: %v != %v", imr, output.NewMerkleRoot)
		}
		if len(output.Proof) != 0 {
			t.Fatalf("expected proof length to be %v but was %v", 0, len(output.Proof))
		}
		if uint64(len(output.Output)) != 0 {
			t.Fatalf("expected output to have len %v but was %v", 0, len(output.Output))
		}
		numOutputs++
	}
	// There should be one output since there was one instruction.
	if numOutputs != 1 {
		t.Fatalf("numOutputs was %v but should be %v", numOutputs, 1)
	}
	// The storage obligation should be unchanged before finalizing the program.
	if len(so.sectorMap) > 0 {
		t.Fatalf("wrong sectorMap len %v > %v", len(so.sectorMap), 0)
	}
	if len(so.sectorRoots) > 0 {
		t.Fatalf("wrong sectorRoots len %v > %v", len(so.sectorRoots), 0)
	}
	// Finalize the program.
	if err := finalize(); err != nil {
		t.Fatal(err)
	}
	// Check the storage obligation again.
	// The storage obligation should be unchanged before finalizing the program.
	if len(so.sectorMap) != 1 {
		t.Fatalf("wrong sectorMap len %v != %v", len(so.sectorMap), 1)
	}
	if len(so.sectorRoots) != 1 {
		t.Fatalf("wrong sectorRoots len %v != %v", len(so.sectorRoots), 1)
	}
	if _, exists := so.sectorMap[appendDataRoot]; !exists {
		t.Fatal("sectorMap contains wrong root")
	}
	if so.sectorRoots[0] != appendDataRoot {
		t.Fatal("sectorRoots contains wrong root")
	}
}

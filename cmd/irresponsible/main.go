package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"go.sia.tech/siad/build"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"io"
	"lukechampine.com/frand"
	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renter"
	"lukechampine.com/us/renter/proto"
	"lukechampine.com/us/renter/renterutil"
	"lukechampine.com/us/renterhost"
	"strings"
)

func getClient() (*renterutil.SiadClient, error) {
	siadPassword, err := build.APIPassword()
	if err != nil {
		return nil, err
	}
	const addr = ":9980"
	siad := renterutil.NewSiadClient(addr, siadPassword)
	return siad, nil
}

func getHost(siad *renterutil.SiadClient) (hostdb.ScannedHost, error) {
	hostPubKey, err := siad.LookupHost("")
	if err != nil {
		return hostdb.ScannedHost{}, err
	}
	hostIP, err := siad.ResolveHostKey(hostPubKey)
	if err != nil {
		return hostdb.ScannedHost{}, err
	}
	return hostdb.Scan(context.Background(), hostIP, hostPubKey)
}

func formContract(siad *renterutil.SiadClient, host hostdb.ScannedHost, key ed25519.PrivateKey, uploadedBytes uint64, downloadedBytes uint64, duration uint64) (renter.Contract, error) {
	uploadFunds := host.UploadBandwidthPrice.Mul64(uploadedBytes)
	downloadFunds := host.DownloadBandwidthPrice.Mul64(downloadedBytes)
	storageFunds := host.StoragePrice.Mul64(uploadedBytes).Mul64(duration)
	totalFunds := uploadFunds.Add(downloadFunds).Add(storageFunds)
	currentHeight, _ := siad.ChainHeight()
	start, end := currentHeight, currentHeight+types.BlockHeight(duration)
	rev, transaction, err := proto.FormContract(siad, siad, key, host, totalFunds, start, end)
	_ = transaction
	if err != nil {
		return renter.Contract{}, err
	}
	contract := renter.Contract{
		HostKey:   rev.HostKey(),
		ID:        rev.ID(),
		RenterKey: key,
	}
	return contract, nil
}

func contractFormation(siad *renterutil.SiadClient) renter.Contract {
	host, err := getHost(siad)
	if err != nil {
		panic(err)
	}
	const uploadedBytes uint64 = 1e9
	const downloadedBytes uint64 = 2e9
	const duration uint64 = 1000
	key := ed25519.NewKeyFromSeed(frand.Bytes(32))
	c, err := formContract(siad, host, key, uploadedBytes, downloadedBytes, duration)
	if err != nil {
		panic(err)
	}
	return c
}

func getSession(siad *renterutil.SiadClient, c renter.Contract) (*proto.Session, error) {
	hostIP, err := siad.ResolveHostKey(c.HostKey)
	if err != nil {
		return nil, err
	}
	currentHeight, err := siad.ChainHeight()
	if err != nil {
		return nil, err
	}
	return proto.NewSession(hostIP, c.HostKey, c.ID, c.RenterKey, currentHeight)
}

func uploading(siad *renterutil.SiadClient, c renter.Contract, reader io.Reader) []crypto.Hash {
	session, err := getSession(siad, c)
	if err != nil {
		panic(err)
	}
	defer session.Close()
	var sector [renterhost.SectorSize]byte
	var roots []crypto.Hash // that's "Sia/crypto", not "crypto" from the stdlib
	for {
		if _, err := io.ReadFull(reader, sector[:]); err == io.EOF {
			break
		}
		root, _ := session.Append(&sector)
		roots = append(roots, root)
	}
	return roots
}

func downloading(siad *renterutil.SiadClient, contract renter.Contract, roots []crypto.Hash, writer io.Writer) {
	session, err := getSession(siad, contract)
	if err != nil {
		panic(err)
	}
	defer session.Close()
	for _, sectorMerkleRoot := range roots {
		err = session.Read(writer, []renterhost.RPCReadRequestSection{{
			MerkleRoot: sectorMerkleRoot,
			Offset:     0,
			Length:     renterhost.SectorSize,
		}})
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	siad, err := getClient()
	if err != nil {
		panic(err)
	}
	c := contractFormation(siad)

	fmt.Printf("contract formed: %s\n", c)
	uploadData := strings.Repeat("c", 1000)
	var reader io.Reader = strings.NewReader(uploadData)
	roots := uploading(siad, c, reader)
	downloadDataBuilder := bytes.NewBufferString("")
	downloading(siad, c, roots, downloadDataBuilder)
	downloadData := downloadDataBuilder.String()
	if uploadData != downloadData {
		fmt.Printf("%v != %v", uploadData, downloadData)
	}
}

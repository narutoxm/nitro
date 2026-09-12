// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package mevfeed

import (
	"context"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func testBlock(number uint64, parent common.Hash) *types.Block {
	return types.NewBlockWithHeader(&types.Header{Number: new(big.Int).SetUint64(number), ParentHash: parent, Time: number, GasLimit: 30_000_000})
}

func readWireFrame(t *testing.T, conn net.Conn) Frame {
	t.Helper()
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatal(err)
	}
	length := binary.BigEndian.Uint32(header[16:20])
	data := append([]byte(nil), header...)
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatal(err)
	}
	data = append(data, payload...)
	f, err := DecodeFrame(data, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestPublisherHelloAndBlockFrames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "feed.sock")
	c := DefaultConfig
	c.Enable, c.SocketPath, c.ChainID = true, path, 42161
	p := NewPublisher(c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer p.StopAndWait()
	var conn net.Conn
	var err error
	for i := 0; i < 20; i++ {
		conn, err = net.Dial("unix", path)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if hello := readWireFrame(t, conn); hello.Kind != FrameHello || hello.Sequence != 1 {
		t.Fatalf("unexpected HELLO: %+v", hello)
	}
	b := testBlock(1, common.Hash{})
	p.TryPublish(b, types.Receipts{})
	if begin := readWireFrame(t, conn); begin.Kind != FrameBlockBegin {
		t.Fatalf("unexpected block begin: %+v", begin)
	}
	if end := readWireFrame(t, conn); end.Kind != FrameBlockEnd {
		t.Fatalf("unexpected block end: %+v", end)
	}
}

func TestPublisherHelloUsesInitialCanonicalHead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "feed.sock")
	c := DefaultConfig
	c.Enable, c.SocketPath, c.ChainID = true, path, 46630
	p := NewPublisher(c)
	head := testBlock(77, common.HexToHash("0x1234"))
	if err := p.SetInitialHead(head.NumberU64(), head.Hash()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer p.StopAndWait()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	hello := readWireFrame(t, conn)
	if hello.Kind != FrameHello || len(hello.Payload) != 89 {
		t.Fatalf("unexpected HELLO: %+v", hello)
	}
	if got := binary.BigEndian.Uint64(hello.Payload[48:56]); got != head.NumberU64() {
		t.Fatalf("HELLO head number = %d", got)
	}
	if got := common.BytesToHash(hello.Payload[56:88]); got != head.Hash() {
		t.Fatalf("HELLO head hash = %s", got)
	}
}

func TestPublisherHelloConsumesExistingStickyGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "feed.sock")
	c := DefaultConfig
	c.Enable, c.SocketPath, c.ChainID = true, path, 46630
	p := NewPublisher(c)
	p.stickyGap.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer p.StopAndWait()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	hello := readWireFrame(t, conn)
	if hello.Kind != FrameHello || len(hello.Payload) != 89 || hello.Payload[88] != 1 {
		t.Fatalf("HELLO did not report sticky gap: %+v", hello)
	}
	deadline := time.Now().Add(time.Second)
	for !p.clientReady.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.stickyGap.Load() {
		t.Fatal("HELLO must consume the previously reported sticky gap")
	}
	p.TryPublish(testBlock(1, common.Hash{}), types.Receipts{})
	if next := readWireFrame(t, conn); next.Kind != FrameBlockBegin {
		t.Fatalf("unexpected stale GAP after HELLO: %+v", next)
	}
}

func TestPublisherQueueOverflowSetsGap(t *testing.T) {
	c := DefaultConfig
	c.Enable = true
	c.QueueSize = 16
	p := NewPublisher(c)
	p.enabled.Store(true)
	for i := uint64(1); i <= uint64(c.QueueSize)+1; i++ {
		p.TryPublish(testBlock(i, common.Hash{}), types.Receipts{})
	}
	if !p.stickyGap.Load() {
		t.Fatal("expected sticky gap after queue overflow")
	}
}

func TestPublisherWriteFrameAtomicallyClaimsStickyGap(t *testing.T) {
	p := NewPublisher(DefaultConfig)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	p.conn = server
	p.clientReady.Store(true)
	p.stickyGap.Store(true)
	done := make(chan bool, 1)
	go func() { done <- p.writeFrame(FrameBlockBegin, []byte{}) }()
	if frame := readWireFrame(t, client); frame.Kind != FrameGap {
		t.Fatalf("first frame must recover the claimed gap, got %v", frame.Kind)
	}
	if frame := readWireFrame(t, client); frame.Kind != FrameBlockBegin {
		t.Fatalf("payload frame was not written after gap recovery, got %v", frame.Kind)
	}
	if !<-done {
		t.Fatal("writeFrame failed")
	}
	if p.stickyGap.Load() {
		t.Fatal("successful gap recovery must not leave a stale sticky gap")
	}
}

func TestPublisherGapRaisedDuringBlockWaitsForNextBoundary(t *testing.T) {
	p := NewPublisher(DefaultConfig)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	p.conn = server
	p.clientReady.Store(true)
	p.enabled.Store(true)
	for i := 0; i < cap(p.ingress); i++ {
		p.ingress <- blockItem{block: testBlock(uint64(i), common.Hash{})}
	}
	tx := types.NewTransaction(
		0,
		common.HexToAddress("0x1234"),
		big.NewInt(1),
		21_000,
		big.NewInt(1),
		nil,
	)
	block := testBlock(1, common.Hash{}).WithBody(types.Body{Transactions: []*types.Transaction{tx}})
	receipt := types.NewReceipt(nil, false, 21_000)
	receipt.TxHash = tx.Hash()
	receipt.EffectiveGasPrice = big.NewInt(1)
	done := make(chan struct{})
	go func() {
		p.publishItem(blockItem{block: block, receipts: types.Receipts{receipt}})
		close(done)
	}()
	if frame := readWireFrame(t, client); frame.Kind != FrameBlockBegin {
		t.Fatalf("unexpected first block frame: %v", frame.Kind)
	}
	// A queue overflow may happen from the execution thread while the
	// serialized block is still being written. The sticky flag must survive,
	// but it must not be injected between BEGIN and the transaction/end frames.
	dropped := make(chan struct{})
	go func() {
		p.TryPublish(testBlock(2, block.Hash()), types.Receipts{})
		close(dropped)
	}()
	<-dropped
	got := []Frame{readWireFrame(t, client), readWireFrame(t, client)}
	<-done
	if got[0].Kind != FrameTransaction || got[1].Kind != FrameBlockEnd {
		t.Fatalf("GAP split an active block: got %v, %v", got[0].Kind, got[1].Kind)
	}
	if !p.stickyGap.Load() {
		t.Fatal("concurrent queue drop was lost while block was being written")
	}
	// The next queued item consumes the GAP at a boundary, closes the old
	// session, and drains stale items. No transaction or block-end frame may
	// follow that GAP on this session.
	nextDone := make(chan struct{})
	go func() {
		p.publishItem(blockItem{block: testBlock(3, block.Hash()), receipts: types.Receipts{}})
		close(nextDone)
	}()
	if frame := readWireFrame(t, client); frame.Kind != FrameGap {
		t.Fatalf("expected boundary GAP after queue drop, got %v", frame.Kind)
	}
	client.SetReadDeadline(time.Now().Add(time.Second))
	var trailing [1]byte
	if _, err := client.Read(trailing[:]); err == nil {
		t.Fatal("old session continued after boundary GAP")
	}
	<-nextDone
	if got := len(p.ingress); got != 0 {
		t.Fatalf("stale ingress items survived GAP recovery: %d", got)
	}
}

func TestPublisherTracksReorgFromObservedHead(t *testing.T) {
	c := DefaultConfig
	c.Enable = true
	p := NewPublisher(c)
	p.enabled.Store(true)
	first := testBlock(0, common.Hash{})
	second := testBlock(2, common.HexToHash("0x1234"))
	p.TryPublish(first, types.Receipts{})
	p.TryPublish(second, types.Receipts{})
	item := <-p.ingress
	if item.reorg != nil {
		t.Fatal("first observed block must not be a reorg")
	}
	item = <-p.ingress
	if item.reorg == nil || item.reorg.oldNum != 0 || item.reorg.newNum != 2 {
		t.Fatalf("expected parent/height mismatch reorg, got %+v", item.reorg)
	}
}

func TestPublisherEmitsStandaloneReorgBoundary(t *testing.T) {
	p := NewPublisher(DefaultConfig)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	p.conn = server
	p.clientReady.Store(true)
	p.enabled.Store(true)
	oldHead := testBlock(10, common.HexToHash("0x01"))
	newHead := testBlock(8, common.HexToHash("0x02"))
	p.TryPublishReorg(oldHead.NumberU64(), oldHead.Hash(), newHead)
	item := <-p.ingress
	if item.block != nil || item.reorg == nil {
		t.Fatalf("expected standalone reorg item, got %+v", item)
	}
	done := make(chan struct{})
	go func() {
		p.publishItem(item)
		close(done)
	}()
	frame := readWireFrame(t, client)
	if frame.Kind != FrameReorg {
		t.Fatalf("expected REORG frame, got %v", frame.Kind)
	}
	<-done
}

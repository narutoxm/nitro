// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package mevfeed

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// CanonicalBlockObserver is intentionally a one-way, non-blocking interface.
// Implementations must never make block execution depend on a consumer.
type CanonicalBlockObserver interface {
	TryPublish(block *types.Block, receipts types.Receipts)
}

// ReorgObserver is an optional extension implemented by feeds that need to
// notify consumers immediately when canonical execution rolls back without a
// replacement block being appended yet. Keeping this separate preserves the
// one-way CanonicalBlockObserver contract for existing observers.
type ReorgObserver interface {
	TryPublishReorg(oldNumber uint64, oldHash common.Hash, newHead *types.Block)
}

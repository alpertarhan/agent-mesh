package main

import (
	"fmt"
	"testing"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func BenchmarkBridgeRowLookup(b *testing.B) {
	for _, size := range []int{8, 64, 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			rows := make([]broker.SessionInfo, size)
			for i := range rows {
				rows[i].ID = fmt.Sprintf("host/%04d", i)
			}
			for b.Loop() {
				for i := size - min(size, bridgeMirrorCap); i < size; i++ {
					if findRow(rows, rows[i].ID) != &rows[i] {
						b.Fatal("wrong row")
					}
				}
			}
		})
	}
}

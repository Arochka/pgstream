// SPDX-License-Identifier: Apache-2.0

package postgres

import "context"

type mapper interface {
	TypeForOID(context.Context, uint32) (string, error)
}

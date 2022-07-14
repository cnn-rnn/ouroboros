package constants

import "math/big"

const PeerDataFile = "peer_data.txt"
const PeerMapFile = "peer_map.txt"

const Active = "active"
const Inactive = "inactive"

var S = "10000000000000000000000000000000000000000000000000000000000000000"
var Smin = "0000000000000000000000000000000000000000000000000000000000000000"
var Smax = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

var Imin = new(big.Int).SetUint64(uint64(0))
var Imax, _ = new(big.Int).SetString(Smax, 16)

var Db0 = new(big.Int).Div(Imax, new(big.Int).SetUint64(100))
var Db1 = new(big.Int).Div(Imax, new(big.Int).SetUint64(1000))
var Db2 = new(big.Int).Div(Imax, new(big.Int).SetUint64(10000000))

var DI2 = new(big.Float).Quo(new(big.Float).SetInt(Imax), new(big.Float).SetInt64(10000000))

package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("server is crashed")
var ERR_NOT_LEADER = fmt.Errorf("server is not the leader")

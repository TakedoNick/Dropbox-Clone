package surfstore

import (
	context "context"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {

	// connect to server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}

	*succ = success.Flag
	// fmt.Printf("succ: %v\n", succ)

	return conn.Close()
	// panic("todo")
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {

	// connect to server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	hashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}

	*blockHashesOut = hashes.Hashes

	return conn.Close()

	// panic("todo")

}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	blockhashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}

	*blockHashes = blockhashes.Hashes

	return conn.Close()
	// panic("todo")
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {

	// connect to server
	for {
		for serverID := range surfClient.MetaStoreAddrs {

			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[serverID], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}

			r := NewRaftSurfstoreClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			// fileinfomap := make(map[string]*FileMetaData)
			f, err := r.GetFileInfoMap(ctx, &emptypb.Empty{})
			if err != nil {
				// Try the next server if server errors and not connection errors
				errMessage, _ := status.FromError(err)
				if errMessage.Message() == ERR_SERVER_CRASHED.Error() || errMessage.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}

				conn.Close()
				return err
			}

			*serverFileInfoMap = f.FileInfoMap

			return conn.Close()

		}
	}

	// panic("todo")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {

	// connect to server
	for {
		for serverID := range surfClient.MetaStoreAddrs {
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[serverID], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}

			r := NewRaftSurfstoreClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			version, err := r.UpdateFile(ctx, fileMetaData)
			if err != nil {
				// Try the next server if server errors and not connection errors
				errMessage, _ := status.FromError(err)
				if errMessage.Message() == ERR_SERVER_CRASHED.Error() || errMessage.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}

				conn.Close()
				return err
			}

			*latestVersion = version.Version

			return conn.Close()
		}
	}

	// panic("todo")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {

	// connect to server
	for {
		for serverID := range surfClient.MetaStoreAddrs {
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[serverID], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}

			r := NewRaftSurfstoreClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			blockServertoblockhash, err := r.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
			if err != nil {
				// Try the next server if server errors and not connection errors
				errMessage, _ := status.FromError(err)
				if errMessage.Message() == ERR_SERVER_CRASHED.Error() || errMessage.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}

				conn.Close()
				return err
			}

			blockhashes := make(map[string][]string)
			for key, value := range blockServertoblockhash.BlockStoreMap {
				blockhashes[key] = value.Hashes
			}

			*blockStoreMap = blockhashes

			return conn.Close()
		}
	}
	// panic("todo")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {

	// connect to server
	for {
		for serverID := range surfClient.MetaStoreAddrs {
			conn, err := grpc.Dial(surfClient.MetaStoreAddrs[serverID], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}

			r := NewRaftSurfstoreClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			b, err := r.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
			if err != nil {
				// Try the next server if server errors and not connection errors
				errMessage, _ := status.FromError(err)
				if errMessage.Message() == ERR_SERVER_CRASHED.Error() || errMessage.Message() == ERR_NOT_LEADER.Error() {
					conn.Close()
					continue
				}

				conn.Close()
				return err
			}

			*blockStoreAddrs = b.BlockStoreAddrs

			return conn.Close()
		}
	}
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}

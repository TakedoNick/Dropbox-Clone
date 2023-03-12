package surfstore

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	// Scan base directory and compute hashlists
	base_dir_files := make(map[string]*FileMetaData)

	// First Time Sync -> create an `index.db` metadata file from the current directory
	if _, err := os.Stat(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)); os.IsNotExist(err) {
		WriteMetaFile(base_dir_files, client.BaseDir)
	}

	// fmt.Println("Loading base directory state.")
	// Capture base directory state <- base_dir_files
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatalf("Error reading from base dir, %v", err)
	}

	for _, file := range files {
		if file.Name() != "index.db" && file.Name() != ".DS_Store" {
			base_dir_files[file.Name()] = &FileMetaData{
				Filename:      file.Name(),
				Version:       int32(0),
				BlockHashList: files2blocks2hashes(client, file.Name())}
		}
	}

	// Load Metamap from index.db <- local_index
	local_index, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		fmt.Println(err)
	}

	if _, err := os.Stat(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)); err == nil {
		fmt.Println("Existing Metafile Loaded.")
	}

	// Compare with server
	new_base_dir_files := downloadFromServer(client, local_index, base_dir_files)

	new_local_index, _ := LoadMetaFromMetaFile(client.BaseDir)

	uploadToServer(client, new_local_index, new_base_dir_files)

	local_index, err = LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		fmt.Println("Error reading the index.db file = ", err)
	}

	deletedFileUpdates(client, local_index, new_base_dir_files)

}

func deletedFileUpdates(client RPCClient, local_index map[string]*FileMetaData, baseDir_files map[string]*FileMetaData) {

	// Check if files in base dir changed from local index

	remote_index := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remote_index)

	// var blockstore_address string

	// client.GetBlockStoreAddr(&blockstore_address)

	for fname, fmeta := range local_index {

		_, ok := baseDir_files[fname]

		var newVersion int32

		if !ok {

			if len(fmeta.BlockHashList) != 1 || fmeta.BlockHashList[0] != "0" {
				// file deleted from base directory
				// record tombstone update
				fmeta = &FileMetaData{
					Filename:      fname,
					Version:       local_index[fname].Version + 1,
					BlockHashList: []string{"0"}}

				client.UpdateFile(fmeta, &newVersion)
				if newVersion != -1 {
					local_index[fname] = fmeta
					WriteMetaFile(local_index, client.BaseDir)
				}

			}

		}
	}

	// fmt.Println("Remote index.db after delete")
	// client.GetFileInfoMap(&remote_index)
	// for fname, fmeta := range remote_index {
	// 	fmt.Println(fname, fmeta.Version, len(fmeta.BlockHashList))
	// }

}

func downloadFromServer(client RPCClient, local_index map[string]*FileMetaData, baseDir_files map[string]*FileMetaData) map[string]*FileMetaData {

	remote_index := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remote_index)

	var blockstore_address []string

	client.GetBlockStoreAddrs(&blockstore_address)

	fmt.Println("Len of remote_index: ", len(remote_index))

	// Check if files in remote index not present in local index

	for fname, fmeta := range remote_index {

		val, ok := local_index[fname]
		// _, ok2 := baseDir_files[fname]

		// fmt.Println("Checking file: ", fname)

		// File in server not present in local index or base dir
		if !ok {

			fmt.Printf("\n\nFile:___ %s ____ not present in local_index", fname)

			// Check for deleted file entry in remote index
			if len(fmeta.BlockHashList) == 1 && fmeta.BlockHashList[0] == "0" {

				// fmt.Println("\n\n File deleted on server.")

				if err := os.Remove(ConcatPath(client.BaseDir, fname)); err != nil {
					// fmt.Println("Deleting local file failed!", err)
					newmeta := FileMetaData{Filename: fname,
						Version:       fmeta.Version,
						BlockHashList: fmeta.BlockHashList}

					local_index[fname] = &newmeta
					baseDir_files[fname] = &newmeta
				} else {
					// fmt.Println("File already deleted.")
					newmeta := FileMetaData{Filename: fname,
						Version:       fmeta.Version,
						BlockHashList: fmeta.BlockHashList}

					local_index[fname] = &newmeta
					baseDir_files[fname] = &newmeta
				}
			} else {

				// fmt.Println("\n\nFile Present on Server, downloading file:", fname)
				var file []byte
				// 1. Download blocks for the file

				blockmap := make(map[string][]string)
				client.GetBlockStoreMap(fmeta.BlockHashList, &blockmap)

				reverseBlockMap := make(map[string]string)
				for _, addr := range blockstore_address {
					blocksinaserver := blockmap[addr]
					for _, block := range blocksinaserver {
						reverseBlockMap[block] = addr
					}
				}

				for _, hash := range fmeta.BlockHashList {

					block := Block{}
					client.GetBlock(hash, reverseBlockMap[hash], &block)
					file = append(file, block.BlockData...)
				}

				err := os.WriteFile(ConcatPath(client.BaseDir, fname), file, 0644)
				if err != nil {
					log.Fatalln("Writing downloaded file failed.")
				}

				// Update local index
				local_index[fname] = fmeta
				baseDir_files[fname] = fmeta
				// WriteMetaFile(local_index, client.BaseDir)
			}

		} else if ok && fmeta.Version > val.Version {

			// file in server present in local index and has a newer version on server
			// fmt.Println("file in server present in local index and has a newer version on server")
			if len(fmeta.BlockHashList) == 1 && fmeta.BlockHashList[0] == "0" {
				if err := os.Remove(ConcatPath(client.BaseDir, fname)); err != nil {
					fmt.Println("Deleting local file failed!", err)
					newmeta := FileMetaData{Filename: fname,
						Version:       fmeta.Version,
						BlockHashList: fmeta.BlockHashList}

					local_index[fname] = &newmeta
					baseDir_files[fname] = &newmeta
				} else {
					// fmt.Println("File already deleted.")
					newmeta := FileMetaData{Filename: fname,
						Version:       fmeta.Version,
						BlockHashList: fmeta.BlockHashList}

					local_index[fname] = &newmeta
					baseDir_files[fname] = &newmeta
				}

			} else {
				var file []byte
				// 1. Download blocks for the file
				blockmap := make(map[string][]string)
				client.GetBlockStoreMap(fmeta.BlockHashList, &blockmap)

				reverseBlockMap := make(map[string]string)
				for _, addr := range blockstore_address {
					blocksinaserver := blockmap[addr]
					for _, block := range blocksinaserver {
						reverseBlockMap[block] = addr
					}
				}

				for _, hash := range fmeta.BlockHashList {

					block := Block{}
					client.GetBlock(hash, reverseBlockMap[hash], &block)
					file = append(file, block.BlockData...)
				}

				os.Remove(ConcatPath(client.BaseDir, fname))
				err := os.WriteFile(ConcatPath(client.BaseDir, fname), file, 0644)
				if err != nil {
					log.Fatalln("Writing downloaded file failed.")
				}

				// Update local index
				local_index[fname] = fmeta
				baseDir_files[fname] = fmeta
				// WriteMetaFile(local_index, client.BaseDir)
			}
		}
	}
	// Save local_index to index.db
	// fmt.Println("Writing updated metadata after downloading to index.db")
	WriteMetaFile(local_index, client.BaseDir)

	// fmt.Println("Remote index.db after download")
	// client.GetFileInfoMap(&remote_index)
	// for fname, fmeta := range remote_index {
	// 	fmt.Println(fname, fmeta.Version, len(fmeta.BlockHashList))
	// }

	return baseDir_files

}

func uploadToServer(client RPCClient, local_index map[string]*FileMetaData, baseDir_files map[string]*FileMetaData) {

	remote_index := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remote_index)

	var blockstore_address []string

	client.GetBlockStoreAddrs(&blockstore_address)

	// Check if New files in the base dir not present in local index or remote index
	for fname, fmeta := range baseDir_files {

		var newVersion int32
		var success bool = false
		val, ok := local_index[fname]
		// _, ok2 := remote_index[fname]

		if !ok {
			// File in base dir but not on local index or remote index

			// fmt.Println("File not present in local index or remote index")
			f, err := os.ReadFile(ConcatPath(client.BaseDir, fname))
			if err != nil {
				fmt.Println(err)
			}

			blockmap := make(map[string][]string)
			client.GetBlockStoreMap(fmeta.BlockHashList, &blockmap)

			reverseBlockMap := make(map[string]string)
			for _, addr := range blockstore_address {
				blocksinaserver := blockmap[addr]
				for _, block := range blocksinaserver {
					reverseBlockMap[block] = addr
				}
			}

			// fmt.Println("Uploading File: ", fname)
			for first := 0; first < len(f); first += client.BlockSize {
				last := first + client.BlockSize
				if last > len(f) {
					last = len(f)
				}
				block := Block{BlockData: f[first:last], BlockSize: int32(len(f[first:last]))}

				blockhash := GetBlockHashString(block.BlockData)

				// Upload blocks of file to blockstore
				client.PutBlock(&block, reverseBlockMap[blockhash], &success)
				if !success {
					fmt.Println("Failed to upload block")
					break
				}
			}

			// fmt.Println("Updating server metadata for file.")
			// Update filemeta data to metastore

			newmeta := &FileMetaData{
				Filename:      fname,
				Version:       1,
				BlockHashList: fmeta.BlockHashList}

			client.UpdateFile(newmeta, &newVersion)

			// Update successful --> update local index
			if newVersion != -1 {
				local_index[fname] = newmeta
				WriteMetaFile(local_index, client.BaseDir)
			}
		} else if ok {
			// File in base dir and server but newer version on local
			if !Compare(fmeta.BlockHashList, val.BlockHashList) {

				// fmt.Println("Newer Version on Local.")
				f, err := os.ReadFile(ConcatPath(client.BaseDir, fname))
				if err != nil {
					fmt.Println(err)
				}

				blockmap := make(map[string][]string)
				client.GetBlockStoreMap(files2blocks2hashes(client, fname), &blockmap)

				reverseBlockMap := make(map[string]string)
				for _, addr := range blockstore_address {
					blocksinaserver := blockmap[addr]
					for _, block := range blocksinaserver {
						reverseBlockMap[block] = addr
					}
				}

				for first := 0; first < len(f); first += client.BlockSize {
					last := first + client.BlockSize
					if last > len(f) {
						last = len(f)
					}
					block := Block{BlockData: f[first:last], BlockSize: int32(len(f[first:last]))}
					blockhash := GetBlockHashString(block.BlockData)
					// Upload blocks of file to blockstore
					var success bool
					client.PutBlock(&block, reverseBlockMap[blockhash], &success)
				}

				// Update filemeta data to metastore
				newmeta := &FileMetaData{
					Filename:      fname,
					Version:       local_index[fname].Version + 1,
					BlockHashList: files2blocks2hashes(client, fname)}

				client.UpdateFile(newmeta, &newVersion)

				// Update successful --> update local index
				if newVersion != -1 {
					local_index[fname] = newmeta
					WriteMetaFile(local_index, client.BaseDir)
				} else {
					continue
				}

			}

		}

	}

	// fmt.Println("Remote index.db after upload")
	// client.GetFileInfoMap(&remote_index)
	// for fname, fmeta := range remote_index {
	// 	fmt.Println(fname, fmeta.Version, len(fmeta.BlockHashList))
	// }
}

// 	} else {
// 		// File in base directory and in server

// 		// Check for local modifications
// 		if Compare(fmeta.BlockHashList, local_index[fname].BlockHashList) {
// 			// no local changes

// 			// Compare file version with server
// 			if val.Version > local_index[fname].Version {

// 				fileblocks := make(map[int][]byte)
// 				f, err := os.ReadFile(ConcatPath(client.BaseDir, fname))
// 				if err != nil {
// 					fmt.Println(err)
// 				}

// 				i := 0
// 				for first := 0; first < len(f); first += client.BlockSize {
// 					last := first + client.BlockSize
// 					if last > len(f) {
// 						last = len(f)
// 					}
// 					block := f[first:last]
// 					fileblocks[i] = block
// 					i += 1
// 				}

// 				// download needed blocks and bring the local file up to date
// 				if len(val.BlockHashList) == len(fmeta.BlockHashList) {
// 					for ind, hash := range val.BlockHashList {

// 						if hash != fmeta.BlockHashList[ind] {

// 							block := Block{}
// 							client.GetBlock(hash, blockstore_address, &block)

// 							fileblocks[ind] = block.BlockData
// 							fmeta.BlockHashList[ind] = hash
// 						}
// 					}

// 				} else if len(val.BlockHashList) > len(fmeta.BlockHashList) {

// 					for ind, hash := range fmeta.BlockHashList {

// 						if hash != val.BlockHashList[ind] {

// 							block := Block{}
// 							client.GetBlock(val.BlockHashList[ind], blockstore_address, &block)

// 							fileblocks[ind] = block.BlockData
// 							fmeta.BlockHashList[ind] = val.BlockHashList[ind]
// 						}
// 					}

// 					for ind := len(fmeta.BlockHashList); ind < len(val.BlockHashList)+1; ind++ {

// 						block := Block{}
// 						client.GetBlock(val.BlockHashList[ind], blockstore_address, &block)

// 						fileblocks[ind] = block.BlockData
// 						fmeta.BlockHashList[ind] = val.BlockHashList[ind]

// 					}

// 				} else {

// 					for ind, hash := range val.BlockHashList {

// 						if hash != fmeta.BlockHashList[ind] {

// 							block := Block{}
// 							client.GetBlock(hash, blockstore_address, &block)

// 							fileblocks[ind] = block.BlockData
// 							fmeta.BlockHashList[ind] = hash
// 						}
// 					}

// 					for ind := len(val.BlockHashList); ind < len(fmeta.BlockHashList)+1; ind++ {
// 						fileblocks[ind] = nil
// 					}

// 				}
// 				// reconstitute file
// 				var file []byte
// 				for _, slice := range fileblocks {
// 					file = append(file, slice...)
// 				}

// 				os.Remove(ConcatPath(client.BaseDir, fname))
// 				err = os.WriteFile(ConcatPath(client.BaseDir, fname), file, 0644)
// 				if err != nil {
// 					log.Fatalln("Error modifying file")
// 				}

// 			}
// 		} else {
// 			// Local changes present

// 			// Compare local index to remote index
// 			if val.Version == local_index[fname].Version {

// 				// Sync local changes to the cloud
// 				fileblocks := make(map[int][]byte)
// 				f, err := os.ReadFile(ConcatPath(client.BaseDir, fname))
// 				if err != nil {
// 					fmt.Println(err)
// 				}

// 				i := 0
// 				for first := 0; first < len(f); first += client.BlockSize {
// 					last := first + client.BlockSize
// 					if last > len(f) {
// 						last = len(f)
// 					}
// 					block := f[first:last]
// 					fileblocks[i] = block
// 					i += 1
// 				}

// 				// download needed blocks and bring the local file up to date
// 				if len(val.BlockHashList) == len(local_index[fname].BlockHashList) {
// 					for ind, hash := range val.BlockHashList {

// 						if hash != local_index[fname].BlockHashList[ind] {
// 							block := Block{BlockData: fileblocks[ind], BlockSize: int32(len(fileblocks[ind]))}
// 							client.PutBlock(&block, blockstore_address, &success)
// 						}
// 					}

// 				} else if len(val.BlockHashList) > len(local_index[fname].BlockHashList) {

// 					for ind, hash := range local_index[fname].BlockHashList {

// 						if hash != val.BlockHashList[ind] {

// 							block := Block{BlockData: fileblocks[ind], BlockSize: int32(len(fileblocks[ind]))}
// 							client.PutBlock(&block, blockstore_address, &success)
// 						}
// 					}

// 					for ind := len(fmeta.BlockHashList); ind < len(val.BlockHashList)+1; ind++ {

// 						block := Block{BlockData: fileblocks[ind], BlockSize: int32(len(fileblocks[ind]))}
// 						client.PutBlock(&block, blockstore_address, &success)
// 					}

// 				} else {

// 					for ind, hash := range val.BlockHashList {

// 						if hash != fmeta.BlockHashList[ind] {

// 							block := Block{BlockData: fileblocks[ind], BlockSize: int32(len(fileblocks[ind]))}
// 							client.PutBlock(&block, blockstore_address, &success)
// 						}
// 					}

// 					for ind := len(val.BlockHashList); ind < len(fmeta.BlockHashList)+1; ind++ {
// 						fileblocks[ind] = nil
// 					}

// 				}

// 			}

// 		}

// 	}

// }

// }

// func compareAndMergeHashLists(client RPCClient, local []string, server []string) () {
// 	for ind, hash := range val.BlockHashList {

// 		if fmeta.BlockHashList[ind] != hash {
// 			block := Block{}
// 			client.GetBlock(hash, blockstore_address, &block)

// 		} else {

// 		}

// 	}
// }

func files2blocks2hashes(client RPCClient, filename string) (hashes []string) {

	f, err := os.ReadFile(ConcatPath(client.BaseDir, filename))
	if err != nil {
		fmt.Println(err)
	}

	for first := 0; first < len(f); first += client.BlockSize {
		last := first + client.BlockSize
		if last > len(f) {
			last = len(f)
		}
		block := f[first:last]
		hashes = append(hashes, GetBlockHashString(block))
	}

	return hashes
}

func Compare(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

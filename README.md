
# Surfstore

## Command to Test
1. Start the server
```bash
go run cmd/SurfstoreServerExec/main.go -s both -p 8081 -l localhost:8081
```
2. sync the base directory
```bash
go run cmd/SurfstoreClientExec/main.go localhost:8081 ./test/test_dir 4096
```
3. sync the other  directory
```bash
go run cmd/SurfstoreClientExec/main.go localhost:8081 ./test/test_dir2 4096
```

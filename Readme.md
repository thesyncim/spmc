SPMC

Generic lock free single producer multiple consumer (adapted from sync.Pool)


``` 
goos: darwin
goarch: amd64
pkg: github.com/thesyncim/spmc
cpu: Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz
BenchmarkChannelSPMC
BenchmarkChannelSPMC-16    	 2906463	       408.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkSPMC
BenchmarkSPMC-16           	995810409	         1.215 ns/op	       0 B/op	       0 allocs/op
PASS

Process finished with the exit code 0
```

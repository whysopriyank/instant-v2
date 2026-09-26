// unameRelease reports the kernel release via uname(2); "unknown" when
// unavailable (e.g. no SYS_UNAME on the build platform).
package main

func unameRelease() string {
	release, ok := unameReleaseSys()
	if !ok || release == "" {
		return "unknown"
	}
	return release
}

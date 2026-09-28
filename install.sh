#!/bin/sh
# Install or upgrade agm from the latest GitHub release:
#   curl -fsSL https://raw.githubusercontent.com/alpertarhan/agent-mesh/main/install.sh | sh
# AGM_INSTALL_DIR overrides the target directory (default ~/.local/bin).
set -eu

dir=${AGM_INSTALL_DIR:-$HOME/.local/bin}
os=$(uname -s | tr '[:upper:]' '[:lower:]')
case $os in darwin | linux) ;; *) echo "agm: unsupported OS $os" >&2; exit 1 ;; esac
case $(uname -m) in
x86_64 | amd64) arch=amd64 ;;
arm64 | aarch64) arch=arm64 ;;
*) echo "agm: unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

file=agm_${os}_${arch}.tar.gz
url=https://github.com/alpertarhan/agent-mesh/releases/latest/download
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

curl -fsSL "$url/$file" -o "$tmp/$file"
curl -fsSL "$url/checksums.txt" -o "$tmp/checksums.txt"
if command -v sha256sum >/dev/null; then sum=$(sha256sum "$tmp/$file"); else sum=$(shasum -a 256 "$tmp/$file"); fi
grep -q "^${sum%% *}  $file\$" "$tmp/checksums.txt" || { echo "agm: checksum mismatch for $file" >&2; exit 1; }

tar -xzf "$tmp/$file" -C "$tmp" agm
mkdir -p "$dir"
mv -f "$tmp/agm" "$dir/agm" # replaces the file: a running daemon notices and restarts itself
chmod 755 "$dir/agm"

echo "installed agm $("$dir/agm" version) to $dir/agm"
case ":$PATH:" in *":$dir:"*) ;; *) echo "note: $dir is not on your PATH" ;; esac
echo "next: agm install   (hooks/adapters for every detected harness)"

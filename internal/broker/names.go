package broker

import (
	"fmt"
	"hash/fnv"
)

// Word lists for default session names ("swift-otter"). 64×64 = 4096 combinations;
// neutral words only, so no pair reads badly.
var (
	adjectives = [64]string{
		"amber", "ancient", "arctic", "bold", "brave", "bright", "calm", "clever",
		"cosmic", "crimson", "curious", "daring", "dusty", "eager", "electric", "emerald",
		"fearless", "fierce", "frosty", "gentle", "gilded", "golden", "hidden", "hollow",
		"iron", "jade", "jolly", "keen", "lively", "lucky", "lunar", "misty",
		"mighty", "nimble", "noble", "silent", "obsidian", "polar", "proud", "quick",
		"quiet", "rapid", "rustic", "sapphire", "scarlet", "shadow", "sharp", "silver",
		"sly", "solar", "steady", "stellar", "stormy", "sunny", "swift", "tidal",
		"turbo", "twilight", "velvet", "vivid", "wild", "wise", "witty", "zesty",
	}
	nouns = [64]string{
		"badger", "bear", "beaver", "bison", "cobra", "condor", "coyote", "crane",
		"dingo", "dolphin", "dragon", "eagle", "falcon", "ferret", "finch", "fox",
		"gecko", "griffin", "hawk", "heron", "husky", "ibis", "jackal", "jaguar",
		"koala", "kraken", "lemur", "leopard", "lion", "llama", "lynx", "mako",
		"mantis", "marten", "moose", "narwhal", "ocelot", "orca", "osprey", "otter",
		"owl", "panda", "panther", "pelican", "phoenix", "puma", "quokka", "raven",
		"rhino", "salmon", "shark", "sparrow", "stag", "tiger", "toucan", "viper",
		"walrus", "weasel", "wolf", "wombat", "yak", "yeti", "zebra", "bobcat",
	}
)

// genName derives a stable name from a session id: the same id always gets the same
// name (across daemon restarts and resumes) unless that name is taken, in which case
// the next free combination is used.
func genName(id string, taken func(string) bool) string {
	h := fnv.New64a()
	h.Write([]byte(id))
	start := h.Sum64()
	const n = uint64(len(adjectives) * len(nouns))
	for i := range n {
		k := (start + i) % n
		name := adjectives[k%uint64(len(adjectives))] + "-" + nouns[k/uint64(len(adjectives))]
		if !taken(name) {
			return name
		}
	}
	return fmt.Sprintf("agent-%x", start) // all 4096 taken
}

# Wikipedia ZIM Extractor

A `rust` based ZIM extractor, parser and enhancer for the Ethereum Swarm Wikipedia Gitcoin bounty.

This extractor:

1. Extracts a `zim` file from `https://dumps.wikimedia.org/other/kiwix/zim/wikipedia/`.
2. Processes extracted files to:

    a. Remove `head` to minimise space.
    b. Rewrites `src` attributes in `img` and `media` tags.
    c. Gzip the data to minimise storage space (~6x - 8x compression factor achieved on `wiki` articles).

# Contributions

The source code was forked from @dignifiedquire with the original being available at https://github.com/dignifiedquire/zim.
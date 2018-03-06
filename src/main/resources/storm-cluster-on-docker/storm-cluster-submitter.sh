docker run \
        --link some-nimbus:nimbus \
        -it --rm \
        -v $(pwd)/$1:/$1 \
        storm storm jar /$1 $2 $3


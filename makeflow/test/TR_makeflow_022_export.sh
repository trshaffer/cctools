#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_INPUT=`basename $0 .sh`.input

export ECHOA=`which echo`
export ECHOB=echoY
export ECHOC=echoX

prepare()
{
cat > ../$TEST_INPUT <<EOF
$ECHOA echob
$ECHOA echob
EOF
    exit 0
}

run()
{
	../src/makeflow syntax/export.external.makeflow
	exec diff ../$TEST_INPUT out.all
}

clean()
{
    rm -f $TEST_INPUT
	rm -f out.all
    exit 0
}

dispatch $@

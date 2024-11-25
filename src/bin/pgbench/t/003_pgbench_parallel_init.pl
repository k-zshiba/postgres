
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Check the initial state of the data generated.  Tables for tellers and
# branches use NULL for their filler attribute.  The table accounts uses
# a non-NULL filler.  The history table should have no data.
sub check_data_state
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	my $node = shift;
	my $type = shift;
	my $scale = shift;

	my $sql_result = $node->safe_psql('postgres',
		'SELECT count(*) AS null_count FROM pgbench_accounts WHERE filler IS NULL LIMIT 10;'
	);
	is($sql_result, '0',
		"$type: filler column of pgbench_accounts has no NULL data");
	$sql_result = $node->safe_psql('postgres',
		'SELECT count(*) AS null_count FROM pgbench_branches WHERE filler IS NULL;'
	);
	is($sql_result, 1 * $scale,
		"$type: filler column of pgbench_branches has only NULL data");
	$sql_result = $node->safe_psql('postgres',
		'SELECT count(*) AS null_count FROM pgbench_tellers WHERE filler IS NULL;'
	);
	is($sql_result, 10 * $scale,
		"$type: filler column of pgbench_tellers has only NULL data");
	$sql_result = $node->safe_psql('postgres',
		'SELECT count(*) AS data_count FROM pgbench_history;');
	is($sql_result, '0', "$type: pgbench_history has no data");
}

# Start a pgbench specific server
my $node = PostgreSQL::Test::Cluster->new('main');
# Set to untraslated messages, to be able to compare program output with
# expeted strings.
$node->init(extra => [ '--locale', 'C' ]);
$node->start;

# check if threads are supported while initialization
my $naccounts = 100000;
my $scale = 10;
# no partition
$node->pgbench(
    '-q -j 2 -i -s 10',
	0,
	[qr{^$}],
	[
		qr{dropping old tables},
		qr{creating tables},
		qr{vacuuming},
		qr{creating primary keys},
		qr{done in \d+\.\d\d s }
	],
	'pgbench parallel initialization without partitions');
# Check data state, after client-side data generation.
check_data_state($node, 'client-side', $scale);
is($node->safe_psql('postgres', 'SELECT count(*) FROM pgbench_accounts;'), $naccounts * $scale, 'parallel copy [no partition]');

# parallel copy into partition table
$node->pgbench(
    '-q -j 2 -i -s 10 --partitions=4',
	0,
	[qr{^$}],
	[
		qr{dropping old tables},
		qr{creating tables},
		qr{creating 4 partitions},
		qr{generating data \(client-side\) by multiple worker threads},
        qr{vacuuming},
		qr{creating primary keys},
		qr{done in \d+\.\d\d s }
	],
	'pgbench parallel initialization with partitions');
# Check data state, after client-side data generation.
check_data_state($node, 'client-side', $scale);
is($node->safe_psql('postgres', 'SELECT count(*) FROM pgbench_accounts;'), $naccounts * $scale, 'parallel copy [partition]');

$node->stop;
done_testing();
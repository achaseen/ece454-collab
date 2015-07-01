use strict;
use warnings;

my $path_to_file = $ARGV[0];

open my $handle, '<', $path_to_file;
chomp(my @lines = <$handle>);
close $handle;

my @result = sort(@lines);

for (@result) {
    print $_ . "\n";
}

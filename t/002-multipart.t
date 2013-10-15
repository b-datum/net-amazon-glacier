#!perl -T

use strict;
use warnings;

use Test::More;
use File::Temp qw(tempfile);
use Data::Printer;

unless ( $ENV{'AMAZON_GLACIER_EXPENSIVE_TESTS'}
  && $ENV{'AWS_ACCESS_KEY_ID'}
  && $ENV{'AWS_ACCESS_KEY_SECRET'}
  && $ENV{'AWS_GLACIER_REGION'} )
{
  plan skip_all => 'Testing this module for real costs money.';
}

BEGIN { use_ok 'Net::Amazon::Glacier'; }

my $glacier = new_ok(
  'Net::Amazon::Glacier' => [
    $ENV{'AWS_GLACIER_REGION'}, $ENV{'AWS_ACCESS_KEY_ID'},
    $ENV{'AWS_ACCESS_KEY_SECRET'}
  ],
);

my $test_vault_name = 'test_vault';
ok( $glacier->create_vault($test_vault_name), 'vault create request ok' );

is(
  (
    scalar grep { $_->{VaultName} eq $test_vault_name }
      @{ $glacier->list_vaults || [] }
  ),
  1,
  'vault created and listed'
);

ok( my $uploads = $glacier->list_multipart_uploads($test_vault_name),
  'get multipart uploads list' );
is( scalar @$uploads, 0, 'no uploads' );

# my ( $fh, $filename ) = tempfile( CLEANUP => 0 );

# my $content = 'x' x (1_024**2 + 200); # 1mb + 200 bytes
# print $fh $content;
# close($fh);

# unlink $filename if -e $filename;
done_testing;

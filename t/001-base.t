#!perl -T

use strict;
use warnings;

use Test::More;
use File::Temp qw(tempfile);

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

my ( $fh, $filename ) = tempfile( CLEANUP => 0 );

my $content = 'x' x 200;
print $fh $content;
close($fh);

ok(
  my $archive_id = $glacier->upload_archive(
    $test_vault_name, $filename, 'short description'
  ),
  'archive sent'
);

ok( $glacier->delete_archive( $test_vault_name, $archive_id ),
  'archive deleted' );

ok( $glacier->delete_vault($test_vault_name), 'vault delete request ok' );
is(
  (
    scalar grep { $_->{VaultName} eq $test_vault_name }
      @{ $glacier->list_vaults || [] }
  ),
  0,
  'vault deleted'
);

unlink $filename if -e $filename;
done_testing;

#!perl -T

use strict;
use warnings;

use Test::More;
use Test::Exception;
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

{
  ok( my $uploads = $glacier->list_multipart_uploads($test_vault_name),
    'get multipart uploads list' );
  is( scalar @$uploads, 0, 'no uploads' );

  # $glacier->abort_multipart_upload( $test_vault_name, $_ )
  #   for map { $_->{MultipartUploadId} } @$uploads;
}

my $upload_id;
diag('Testing exceptions');

{
  throws_ok { $glacier->initiate_multipart_upload($test_vault_name) }
  qr/part_size is required/, 'Missing $part_size throws ok';
}

{
  throws_ok { $glacier->initiate_multipart_upload( $test_vault_name, 1 ) }
  qr/part_size smaller/, 'Small $part_size throws ok';
}
{
  throws_ok {
    $glacier->initiate_multipart_upload( $test_vault_name, 9 * 1_024**3 );
  }
  qr/part_size bigger/, 'Big $part_size throws ok';
}

diag('Initiating multipart upload');

{
  ok(
    $upload_id = $glacier->initiate_multipart_upload(
      $test_vault_name, 1_024**2, 'some description'
    ),
    'multipart upload initiated'
  );
}
{
  ok( my $uploads = $glacier->list_multipart_uploads($test_vault_name),
    'get multipart uploads list' );
  is( scalar @$uploads, 1, '1 upload' );
  ok( scalar( grep { $_->{MultipartUploadId} eq $upload_id } @$uploads ),
    '$upload_id found' );
}

diag 'Aborting...';
{
  ok( $glacier->abort_multipart_upload( $test_vault_name, $upload_id ),
    'multipart upload aborted successfuly' );
}
{
  ok( my $uploads = $glacier->list_multipart_uploads($test_vault_name),
    'get multipart uploads list' );
  is( scalar @$uploads, 0, '0 uploads - aborted successfuly' );
}

# my ( $fh, $filename ) = tempfile( CLEANUP => 0 );

# my $content = 'x' x (1_024**2 + 200); # 1mb + 200 bytes
# print $fh $content;
# close($fh);

# unlink $filename if -e $filename;
done_testing;

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

  $glacier->abort_multipart_upload( $test_vault_name, $_ )
    for map { $_->{MultipartUploadId} } @$uploads;
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

{

  my $part_1 = 'c' x 1_024**2;
  my $part_2 = 'c' x 200;
  my $size   = length($part_1);

  ok(
    $upload_id = $glacier->initiate_multipart_upload(
      $test_vault_name, $size, 'desc'
    ),
    'multipart upload initiated'
  );

  my @trees;
  my $current_tree_hash;

  ok(
    $current_tree_hash = $glacier->put_part(
      $test_vault_name, $upload_id, $part_1, 0, length($part_1) - 1
    ),
    'multipart part 1 sent'
  );

  push @trees, $current_tree_hash;

  ok(
    $current_tree_hash = $glacier->put_part(
      $test_vault_name, $upload_id,
      $part_2,          length($part_1),
      length( $part_1 . $part_2 ) - 1
    ),
    'multipart part 2 sent'
  );

  push @trees, $current_tree_hash;

  ok(
    my $archive_id = $glacier->complete_multipart_upload(
      $test_vault_name, $upload_id, length( $part_1 . $part_2 ), @trees
    )
  );

}

done_testing;

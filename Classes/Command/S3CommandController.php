<?php
namespace Flownative\Aws\S3\Command;

/*                                                                        *
 * This script belongs to the package "Flownative.Aws.S3".                *
 *                                                                        */

use Aws\S3\BatchDelete;
use Aws\S3\Model\ClearBucket;
use Aws\S3\S3Client;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;

/**
 * S3 command controller for the Flownative.Aws.S3 package
 *
 * @Flow\Scope("singleton")
 */
class S3CommandController extends CommandController
{
    /**
     * @Flow\InjectConfiguration("profiles.default")
     * @var array
     */
    protected $s3DefaultProfile;

    /**
     * Checks the connection
     *
     * This command checks if the configured credentials and connectivity allows for connecting with the S3 web service.
     *
     * By default, this command will run the "list buckets" operation on the S3 web service. If your IAM policy does
     * not allow listing buckets, you can pass a specific bucket through the "--bucket" argument. In that case, this
     * command will try to retrieve meta data only for that given bucket using the "head bucket" operation.
     *
     * @param string $bucket If specified, we try to connect by retrieving meta data for this specific bucket only
     * @param string $prefix
     * @return void
     */
    public function connectCommand($bucket = null, $prefix = '')
    {
        try {
            $s3Client = new S3Client($this->s3DefaultProfile);
            if ($bucket !== null) {
                $s3Client->registerStreamWrapper();

                $this->outputLine('Access list of objects in bucket "%s" with key prefix "%s" ...', [$bucket, $prefix]);
                $s3Client->getPaginator('ListObjects', ['Bucket' => $bucket, 'Prefix' => $prefix]);

                $options = array(
                    'Bucket' => $bucket,
                    'Body' => 'test',
                    'ContentLength' => 4,
                    'ContentType' => 'text/plain',
                    'Key' => $prefix . 'Flownative.Aws.S3.ConnectionTest.txt'
                );
                $this->outputLine('Writing test object into bucket (arn:aws:s3:::%s/%s) ...', [$bucket, $options['Key']]);
                $s3Client->putObject($options);

                $this->outputLine('Deleting test object from bucket ...');
                $options = array(
                    'Bucket' => $bucket,
                    'Key' => $prefix . 'Flownative.Aws.S3.ConnectionTest.txt'
                );
                $s3Client->deleteObject($options);
            } else {
                $s3Client->listBuckets();
            }
        } catch (\Exception $e) {
            $this->outputLine('<b>' . $e->getMessage() . '</b>');
            if ($bucket === null || $prefix === '') {
                $this->outputLine('Hint: Maybe your IAM policy restricts the user from listing all buckets. In that case, try using the "--bucket" and "--prefix" arguments.');
            }
            $this->quit(1);
        }
        $this->outputLine('OK');
    }

    /**
     * Displays a list of buckets
     *
     * This command outputs a list of all buckets from the currently configured S3
     * account.
     *
     * @return void
     */
    public function listBucketsCommand()
    {
        try {
            $s3Client = new S3Client($this->s3DefaultProfile);
            $result = $s3Client->listBuckets();
        } catch (\Exception $e) {
            $this->outputLine($e->getMessage());
            $this->quit(1);
            exit;
        }

        if (count($result['Buckets']) === 0) {
            $this->outputLine('The account currently does not have any buckets.');
        }

        $tableRows = array();
        $headerRow = array('Bucket Name', 'Creation Date');

        foreach ($result['Buckets'] as $bucket) {
            $tableRows[] = array($bucket['Name'], $bucket['CreationDate']);
        }

        $this->output->outputTable($tableRows, $headerRow);
    }

    /**
     * Removes all objects from a bucket
     *
     * This command deletes all objects (files) of the given bucket.
     *
     * @param string $bucket Name of the bucket
     * @return void
     */
    public function flushBucketCommand($bucket)
    {
        try {
            $s3Client = new S3Client($this->s3DefaultProfile);
            $batchDelete = BatchDelete::fromListObjects($s3Client, ['Bucket' => $bucket]);
            $promise = $batchDelete->promise();
        } catch (\Exception $e) {
            $this->outputLine($e->getMessage());
            $this->quit(1);
            exit;
        }
        $promise->wait();
        $this->outputLine('Successfully flushed bucket %s.', array($bucket));
    }

    /**
     * Upload a file to a bucket
     *
     * This command uploads the file specified by <b>file</b> to the bucket
     * specified by <bucket>. The bucket must exist already in order to upload
     * the file.
     *
     * @param string $bucket Name of the bucket
     * @param string $file Full path leading to the file to upload
     * @param string $key Key to use for the uploaded object, for example "Coffee.jpg" or "MyPictures/Machines/Coffee.jpg". If not specified, the original filename is used.
     * @return void
     */
    public function uploadCommand($bucket, $file, $key = '')
    {
        if (!file_exists($file)) {
            $this->outputLine('The specified file does not exist.');
            $this->quit(1);
        }

        if ($key === '') {
            $key = basename($file);
        }

        try {
            $s3Client = new S3Client($this->s3DefaultProfile);
            $s3Client->putObject(array(
                'Key' => $key,
                'Bucket' => $bucket,
                'Body' => fopen('file://' . realpath($file), 'rb')
            ));
        } catch (\Exception $e) {
            $this->outputLine('Could not upload %s to %s::%s – %s', array($file, $bucket, $key, $e->getMessage()));
            $this->quit(1);
        }

        $this->outputLine('Successfully uploaded %s to %s::%s.', array($file, $bucket, $key));
    }
}

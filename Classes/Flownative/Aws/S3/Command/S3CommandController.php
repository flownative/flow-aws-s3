<?php
namespace Flownative\Aws\S3\Command;

/*                                                                        *
 * This script belongs to the package "Flownative.Aws.S3".                *
 *                                                                        *
 *                                                                        */

use Aws\S3\Exception\NoSuchBucketException;
use Aws\S3\Model\ClearBucket;
use Aws\S3\S3Client;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Cli\CommandController;

/**
 * S3 command controller for the Flownative.Aws.S3 package
 *
 * @Flow\Scope("singleton")
 */
class S3CommandController extends CommandController {

	/**
	 * Checks the connection
	 *
	 * This command checks if the configured credentials and connectivity allows for
	 * connecting with the S3 web service.
	 *
	 * @return void
	 */
	public function connectCommand() {
		try {
			$s3Client = S3Client::factory();
			$s3Client->headBucket(array('Bucket' => 'some-dummy-bucket'));
		}
		catch (NoSuchBucketException $e) {
		}
		catch (\Exception $e) {
			$this->outputLine($e->getMessage());
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
	public function listBucketsCommand() {
		try {
			$s3Client = S3Client::factory();
			$result = $s3Client->listBuckets();
		} catch(\Exception $e) {
			$this->outputLine($e->getMessage());
			$this->quit(1);
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
	public function flushBucketCommand($bucket) {
		try {
			$s3Client = S3Client::factory();
			$clearBucket = new ClearBucket($s3Client, $bucket);
			$clearBucket->clear();
		} catch(\Exception $e) {
			$this->outputLine($e->getMessage());
			$this->quit(1);
		}
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
	public function uploadCommand($bucket, $file, $key = '') {
		if (!file_exists($file)) {
			$this->outputLine('The specified file does not exist.');
			$this->quit(1);
		}

		if ($key === '') {
			$key = basename($file);
		}

		try {
			$s3Client = S3Client::factory();
			$s3Client->putObject(array(
				'Key' => $key,
				'Bucket' => $bucket,
				'Body' => fopen('file://' . realpath($file), 'rb')
			));
		} catch(\Exception $e) {
			$this->outputLine('Could not upload %s to %s::%s – %s', array($file, $bucket, $key, $e->getMessage()));
			$this->quit(1);
		}

		$this->outputLine('Successfully uploaded %s to %s::%s.', array($file, $bucket, $key));
	}

}

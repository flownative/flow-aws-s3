<?php
namespace Flownative\Aws\S3;

/*                                                                        *
 * This script belongs to the package "Flownative.Aws.S3".                *
 *                                                                        *
 *                                                                        */

use Aws\S3\S3Client;
use Guzzle\Http\EntityBody;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Resource\CollectionInterface;
use TYPO3\Flow\Resource\Resource;
use TYPO3\Flow\Resource\ResourceManager;
use TYPO3\Flow\Resource\ResourceRepository;
use TYPO3\Flow\Resource\Storage\Exception;
use TYPO3\Flow\Resource\Storage\Object;
use TYPO3\Flow\Resource\Storage\WritableStorageInterface;
use TYPO3\Flow\Utility\Environment;

/**
 * A resource storage based on AWS S3
 */
class S3Storage implements WritableStorageInterface {

	/**
	 * Name which identifies this resource storage
	 *
	 * @var string
	 */
	protected $name;

	/**
	 * Name of the S3 bucket which should be used as a storage
	 *
	 * @var string
	 */
	protected $bucketName;

	/**
	 * A prefix to use for the key of bucket objects used by this storage
	 *
	 * @var string
	 */
	protected $keyPrefix;

	/**
	 * @Flow\Inject
	 * @var Environment
	 */
	protected $environment;

	/**
	 * @Flow\Inject
	 * @var ResourceManager
	 */
	protected $resourceManager;

	/**
	 * @Flow\Inject
	 * @var ResourceRepository
	 */
	protected $resourceRepository;

	/**
	 * @var S3Client
	 */
	protected $s3Client;

	/**
	 * @Flow\InjectConfiguration("profiles.default")
	 * @var array
	 */
	protected $s3DefaultProfile;

	/**
	 * @Flow\Inject
	 * @var \TYPO3\Flow\Log\SystemLoggerInterface
	 */
	protected $systemLogger;

	/**
	 * Constructor
	 *
	 * @param string $name Name of this storage instance, according to the resource settings
	 * @param array $options Options for this storage
	 * @throws Exception
	 */
	public function __construct($name, array $options = array()) {
		$this->name = $name;
		$this->bucketName = $name;
		foreach ($options as $key => $value) {
			switch ($key) {
				case 'bucket':
					$this->bucketName = $value;
					break;
				case 'keyPrefix':
					$this->keyPrefix = $value;
					break;
				default:
					if ($value !== NULL) {
						throw new Exception(sprintf('An unknown option "%s" was specified in the configuration of the "%s" resource S3Storage. Please check your settings.', $key, $name), 1428928229);
					}
			}
		}
	}

	/**
	 * Initialize the S3 Client
	 *
	 * @return void
	 */
	public function initializeObject() {
		$clientOptions = $this->s3DefaultProfile;

		$this->s3Client = S3Client::factory($clientOptions);
		$this->s3Client->registerStreamWrapper();
	}

	/**
	 * Returns the instance name of this storage
	 *
	 * @return string
	 */
	public function getName() {
		return $this->name;
	}

	/**
	 * Returns the S3 bucket name used as a storage
	 *
	 * @return string
	 */
	public function getBucketName() {
		return $this->bucketName;
	}

	/**
	 * Returns the S3 object key prefix
	 *
	 * @return string
	 */
	public function getKeyPrefix() {
		return $this->keyPrefix;
	}

	/**
	 * Imports a resource (file) from the given URI or PHP resource stream into this storage.
	 *
	 * On a successful import this method returns a Resource object representing the newly
	 * imported persistent resource.
	 *
	 * @param string | resource $source The URI (or local path and filename) or the PHP resource stream to import the resource from
	 * @param string $collectionName Name of the collection the new Resource belongs to
	 * @return Resource A resource object representing the imported resource
	 * @throws \TYPO3\Flow\Resource\Storage\Exception
	 */
	public function importResource($source, $collectionName) {
		$temporaryTargetPathAndFilename = $this->environment->getPathToTemporaryDirectory() . uniqid('Flownative_Aws_S3_');

		if (is_resource($source)) {
			try {
				$target = fopen($temporaryTargetPathAndFilename, 'wb');
				stream_copy_to_stream($source, $target);
				fclose($target);
			} catch (\Exception $e) {
				throw new Exception(sprintf('Could import the content stream to temporary file "%s".', $temporaryTargetPathAndFilename), 1428915486);
			}
		} else {
			try {
				copy($source, $temporaryTargetPathAndFilename);
			} catch (\Exception $e) {
				throw new Exception(sprintf('Could not copy the file from "%s" to temporary file "%s".', $source, $temporaryTargetPathAndFilename), 1428915488);
			}
		}

		return $this->importTemporaryFile($temporaryTargetPathAndFilename, $collectionName);
	}

	/**
	 * Imports a resource from the given string content into this storage.
	 *
	 * On a successful import this method returns a Resource object representing the newly
	 * imported persistent resource.
	 *
	 * The specified filename will be used when presenting the resource to a user. Its file extension is
	 * important because the resource management will derive the IANA Media Type from it.
	 *
	 * @param string $content The actual content to import
	 * @return Resource A resource object representing the imported resource
	 * @param string $collectionName Name of the collection the new Resource belongs to
	 * @return Resource A resource object representing the imported resource
	 * @throws Exception
	 * @api
	 */
	public function importResourceFromContent($content, $collectionName) {
		$sha1Hash = sha1($content);
		$md5Hash = md5($content);
		$filename = $sha1Hash;

		$resource = new Resource();
		$resource->setFilename($filename);
		$resource->setFileSize(strlen($content));
		$resource->setCollectionName($collectionName);
		$resource->setSha1($sha1Hash);
		$resource->setMd5($md5Hash);

		$this->s3Client->putObject(array(
			'Bucket' => $this->bucketName,
			'Body' => $content,
			'ContentLength' => $resource->getFileSize(),
			'ContentType' => $resource->getMediaType(),
			'Key' => $this->keyPrefix . $sha1Hash
		));

		return $resource;
	}

	/**
	 * Imports a resource (file) as specified in the given upload info array as a
	 * persistent resource.
	 *
	 * On a successful import this method returns a Resource object representing
	 * the newly imported persistent resource.
	 *
	 * @param array $uploadInfo An array detailing the resource to import (expected keys: name, tmp_name)
	 * @param string $collectionName Name of the collection this uploaded resource should be part of
	 * @return string A resource object representing the imported resource
	 * @throws Exception
	 * @api
	 */
	public function importUploadedResource(array $uploadInfo, $collectionName) {
		$pathInfo = pathinfo($uploadInfo['name']);
		$originalFilename = $pathInfo['basename'];
		$sourcePathAndFilename = $uploadInfo['tmp_name'];

		if (!file_exists($sourcePathAndFilename)) {
			throw new Exception(sprintf('The temporary file "%s" of the file upload does not exist (anymore).', $sourcePathAndFilename), 1428909075);
		}

		$newSourcePathAndFilename = $this->environment->getPathToTemporaryDirectory() . 'Flownative_Aws_S3_' . uniqid() . '.tmp';
		if (move_uploaded_file($sourcePathAndFilename, $newSourcePathAndFilename) === FALSE) {
			throw new Exception(sprintf('The uploaded file "%s" could not be moved to the temporary location "%s".', $sourcePathAndFilename, $newSourcePathAndFilename), 1428909076);
		}

		$sha1Hash = sha1_file($newSourcePathAndFilename);
		$md5Hash = md5_file($newSourcePathAndFilename);

		$resource = new Resource();
		$resource->setFilename($originalFilename);
		$resource->setCollectionName($collectionName);
		$resource->setFileSize(filesize($newSourcePathAndFilename));
		$resource->setSha1($sha1Hash);
		$resource->setMd5($md5Hash);

		$this->s3Client->putObject(array(
			'Bucket' => $this->bucketName,
			'Body' => fopen($newSourcePathAndFilename, 'rb'),
			'ContentLength' => $resource->getFileSize(),
			'ContentType' => $resource->getMediaType(),
			'Key' => $this->keyPrefix . $sha1Hash
		));

		return $resource;
	}

	/**
	 * Deletes the storage data related to the given Resource object
	 *
	 * @param \TYPO3\Flow\Resource\Resource $resource The Resource to delete the storage data of
	 * @return boolean TRUE if removal was successful
	 * @api
	 */
	public function deleteResource(Resource $resource) {
		$this->s3Client->deleteObject(array(
			'Bucket' => $this->bucketName,
			'Key' => $this->keyPrefix . $resource->getSha1()
		));
		return TRUE;
	}

	/**
	 * Returns a stream handle which can be used internally to open / copy the given resource
	 * stored in this storage.
	 *
	 * @param \TYPO3\Flow\Resource\Resource $resource The resource stored in this storage
	 * @return resource | boolean A URI (for example the full path and filename) leading to the resource file or FALSE if it does not exist
	 * @api
	 */
	public function getStreamByResource(Resource $resource) {
		return fopen('s3://' . $this->bucketName . '/' . $this->keyPrefix . $resource->getSha1(), 'r');
	}

	/**
	 * Returns a stream handle which can be used internally to open / copy the given resource
	 * stored in this storage.
	 *
	 * @param string $relativePath A path relative to the storage root, for example "MyFirstDirectory/SecondDirectory/Foo.css"
	 * @return resource | boolean A URI (for example the full path and filename) leading to the resource file or FALSE if it does not exist
	 * @api
	 */
	public function getStreamByResourcePath($relativePath) {
		return fopen('s3://' . $this->bucketName . '/' . $this->keyPrefix . ltrim('/', $relativePath), 'r');
	}

	/**
	 * Retrieve all Objects stored in this storage.
	 *
	 * @return array<\TYPO3\Flow\Resource\Storage\Object>
	 * @api
	 */
	public function getObjects() {
		$objects = array();
		foreach ($this->resourceManager->getCollectionsByStorage($this) as $collection) {
			$objects = array_merge($objects, $this->getObjectsByCollection($collection));
		}
		return $objects;
	}

	/**
	 * Retrieve all Objects stored in this storage, filtered by the given collection name
	 *
	 * @param CollectionInterface $collection
	 * @internal param string $collectionName
	 * @return array<\TYPO3\Flow\Resource\Storage\Object>
	 * @api
	 */
	public function getObjectsByCollection(CollectionInterface $collection) {
		$objects = array();
		$that = $this;
		$bucketName = $this->bucketName;

		foreach ($this->resourceRepository->findByCollectionName($collection->getName()) as $resource) {
			/** @var \TYPO3\Flow\Resource\Resource $resource */
			$object = new Object();
			$object->setFilename($resource->getFilename());
			$object->setSha1($resource->getSha1());
			$object->setStream(function () use ($that, $bucketName, $resource) { return fopen('s3://' . $bucketName . '/' . $this->keyPrefix . $resource->getSha1(), 'r'); } );
			$objects[] = $object;
		}

		return $objects;
	}

	/**
	 * Imports the given temporary file into the storage and creates the new resource object.
	 *
	 * @param string $temporaryPathAndFilename Path and filename leading to the temporary file
	 * @param string $collectionName Name of the collection to import into
	 * @return Resource The imported resource
	 */
	protected function importTemporaryFile($temporaryPathAndFilename, $collectionName) {
		$sha1Hash = sha1_file($temporaryPathAndFilename);
		$md5Hash = md5_file($temporaryPathAndFilename);

		$resource = new Resource();
		$resource->setFileSize(filesize($temporaryPathAndFilename));
		$resource->setCollectionName($collectionName);
		$resource->setSha1($sha1Hash);
		$resource->setMd5($md5Hash);

		$objectName = $this->keyPrefix . $sha1Hash;
		$options = array(
			'Bucket' => $this->bucketName,
			'Body' => fopen($temporaryPathAndFilename, 'rb'),
			'ContentLength' => $resource->getFileSize(),
			'ContentType' => $resource->getMediaType(),
			'Key' => $objectName
		);

		if (!$this->s3Client->doesObjectExist($this->bucketName, $this->keyPrefix . $sha1Hash)) {
			$this->s3Client->putObject($options);
			$this->systemLogger->log(sprintf('Successfully imported resource as object "%s" into bucket "%s" with MD5 hash "%s"', $objectName, $this->bucketName, $resource->getMd5() ?: 'unknown'), LOG_INFO);
		} else {
			$this->systemLogger->log(sprintf('Did not import resource as object "%s" into bucket "%s" because that object already existed.', $objectName, $this->bucketName), LOG_INFO);
		}

		return $resource;
	}

}

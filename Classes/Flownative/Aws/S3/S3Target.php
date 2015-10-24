<?php
namespace Flownative\Aws\S3;

/*                                                                        *
 * This script belongs to the package "Flownative.Aws.S3".                *
 *                                                                        *
 *                                                                        */

use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Resource\Collection;
use TYPO3\Flow\Resource\CollectionInterface;
use TYPO3\Flow\Resource\Exception;
use TYPO3\Flow\Resource\Resource;
use TYPO3\Flow\Resource\ResourceManager;
use TYPO3\Flow\Resource\ResourceMetaDataInterface;
use TYPO3\Flow\Resource\Target\TargetInterface;

/**
 * A resource publishing target based on Amazon S3
 */
class S3Target implements TargetInterface {

	/**
	 * Name which identifies this resource target
	 *
	 * @var string
	 */
	protected $name;

	/**
	 * Name of the S3 bucket which should be used for publication
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
	 * CORS (Cross-Origin Resource Sharing) allowed origins for published content
	 *
	 * @var string
	 */
	protected $corsAllowOrigin = '*';

	/**
	 * @var string
	 */
	protected $baseUri;

	/**
	 * Internal cache for known storages, indexed by storage name
	 *
	 * @var array<\TYPO3\Flow\Resource\Storage\StorageInterface>
	 */
	protected $storages = array();

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
	 * @Flow\Inject
	 * @var ResourceManager
	 */
	protected $resourceManager;

	/**
	 * @var array
	 */
	protected $existingObjectsInfo;

	/**
	 * Constructor
	 *
	 * @param string $name Name of this target instance, according to the resource settings
	 * @param array $options Options for this target
	 * @throws Exception
	 */
	public function __construct($name, array $options = array()) {
		$this->name = $name;
		foreach ($options as $key => $value) {
			switch ($key) {
				case 'bucket':
					$this->bucketName = $value;
					break;
				case 'keyPrefix':
					$this->keyPrefix = $value;
					break;
				case 'corsAllowOrigin':
					$this->corsAllowOrigin = $value;
					break;
				case 'baseUri':
					$this->baseUri = $value;
					break;
				default:
					if ($value !== NULL) {
						throw new Exception(sprintf('An unknown option "%s" was specified in the configuration of the "%s" resource S3Target. Please check your settings.', $key, $name), 1428928226);
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
	 * Returns the name of this target instance
	 *
	 * @return string The target instance name
	 */
	public function getName() {
		return $this->name;
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
	 * Publishes the whole collection to this target
	 *
	 * @param \TYPO3\Flow\Resource\Collection $collection The collection to publish
	 * @return void
	 * @throws Exception
	 */
	public function publishCollection(Collection $collection) {
		if (!isset($this->existingObjectsInfo)) {
			$this->existingObjectsInfo = array();
			$requestArguments = array(
				'Bucket' => $this->bucketName,
				'Delimiter' => '/',
				'Prefix' => $this->keyPrefix
			);

			do {
				$result = $this->s3Client->listObjects($requestArguments);
				$this->existingObjectsInfo[] = $result->get('Contents');
				if ($result->get('IsTruncated')) {
					$requestArguments['marker'] = $result->get('NextMarker');
				}
			} while ($result->get('IsTruncated'));
		}

		$obsoleteObjects = array_fill_keys(array_keys($this->existingObjectsInfo), TRUE);

		$storage = $collection->getStorage();
		if ($storage instanceof S3Storage) {
			$storageBucketName = $storage->getBucketName();
			if ($storageBucketName === $this->bucketName) {
				throw new Exception(sprintf('Could not publish collection %s because the source and target S3 bucket is the same.', $collection->getName()), 1428929137);
			}
			foreach ($collection->getObjects() as $object) {
				/** @var \TYPO3\Flow\Resource\Storage\Object $object */
				$objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($object);
				$options = array(
					'ACL' => 'public-read',
					'Bucket' => $this->bucketName,
					'CopySource' => urlencode($storageBucketName . '/' . $storage->getKeyPrefix() . $object->getSha1()),
					'ContentType' => $object->getMediaType(),
					'MetadataDirective' => 'REPLACE',
					'Key' => $objectName
				);
				try {
					$this->s3Client->copyObject($options);
				} catch (S3Exception $e) {
					throw new Exception(sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $object->getSha1(), $collection->getName(), $storageBucketName, $this->bucketName , $e->getMessage()), 1431009234);
				}
				$this->systemLogger->log(sprintf('Successfully copied resource as object "%s" (MD5: %s) from bucket "%s" to bucket "%s"', $objectName, $object->getMd5() ?: 'unknown', $storageBucketName, $this->bucketName), LOG_DEBUG);
				unset($obsoleteObjects[$this->getRelativePublicationPathAndFilename($object)]);
			}
		} else {
			foreach ($collection->getObjects() as $object) {
				/** @var \TYPO3\Flow\Resource\Storage\Object $object */
				$this->publishFile($object->getStream(), $this->getRelativePublicationPathAndFilename($object), $object);
				unset($obsoleteObjects[$this->getRelativePublicationPathAndFilename($object)]);
			}
		}

		foreach (array_keys($obsoleteObjects) as $relativePathAndFilename) {
			$this->s3Client->deleteObject(array(
				'Bucket' => $this->bucketName,
				'Key' => $this->keyPrefix . $relativePathAndFilename
			));
		}
	}

	/**
	 * Returns the web accessible URI pointing to the given static resource
	 *
	 * @param string $relativePathAndFilename Relative path and filename of the static resource
	 * @return string The URI
	 */
	public function getPublicStaticResourceUri($relativePathAndFilename) {
		return $this->s3Client->getObjectUrl($this->bucketName, $this->keyPrefix . $relativePathAndFilename);
	}

	/**
	 * Publishes the given persistent resource from the given storage
	 *
	 * @param \TYPO3\Flow\Resource\Resource $resource The resource to publish
	 * @param CollectionInterface $collection The collection the given resource belongs to
	 * @return void
	 * @throws Exception
	 */
	public function publishResource(Resource $resource, CollectionInterface $collection) {
		$storage = $collection->getStorage();
		if ($storage instanceof S3Storage) {
			if ($storage->getBucketName() === $this->bucketName) {
				throw new Exception(sprintf('Could not publish resource with SHA1 hash %s of collection %s because the source and target S3 bucket is the same.', $resource->getSha1(), $collection->getName()), 1428929563);
			}
			try {
				$sourceObjectArn = $storage->getBucketName() . '/' . $storage->getKeyPrefix() . $resource->getSha1();
				$objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
				$options = array(
					'ACL' => 'public-read',
					'Bucket' => $this->bucketName,
					'CopySource' => urlencode($sourceObjectArn),
					'ContentType'=> $resource->getMediaType(),
					'MetadataDirective' => 'REPLACE',
					'Key' => $objectName
				);
				$this->s3Client->copyObject($options);
				$this->systemLogger->log(sprintf('Successfully published resource as object "%s" (MD5: %s) by copying from bucket "%s" to bucket "%s"', $objectName, $resource->getMd5() ?: 'unknown', $storage->getBucketName(), $this->bucketName), LOG_DEBUG);
			} catch (S3Exception $e) {
				throw new Exception(sprintf('Could not publish resource with SHA1 hash %s of collection %s (source object: %s) through "CopyObject" because the S3 client reported an error: %s', $resource->getSha1(), $collection->getName(), $sourceObjectArn, $e->getMessage()), 1428999574);
			}
		} else {
			$sourceStream = $resource->getStream();
			if ($sourceStream === FALSE) {
				throw new Exception(sprintf('Could not publish resource with SHA1 hash %s of collection %s because there seems to be no corresponding data in the storage.', $resource->getSha1(), $collection->getName()), 1428929649);
			}
			$this->publishFile($sourceStream, $this->getRelativePublicationPathAndFilename($resource), $resource);
		}
	}

	/**
	 * Unpublishes the given persistent resource
	 *
	 * @param \TYPO3\Flow\Resource\Resource $resource The resource to unpublish
	 * @return void
	 */
	public function unpublishResource(Resource $resource) {
		try {
			$objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
			$this->s3Client->deleteObject(array(
				'Bucket' => $this->bucketName,
				'Key' => $objectName
			));
			$this->systemLogger->log(sprintf('Successfully unpublished resource as object "%s" (MD5: %s) from bucket "%s"', $objectName, $resource->getMd5() ?: 'unknown',$this->bucketName), LOG_DEBUG);
		} catch (\Exception $e) {
		}
	}

	/**
	 * Returns the web accessible URI pointing to the specified persistent resource
	 *
	 * @param \TYPO3\Flow\Resource\Resource $resource Resource object or the resource hash of the resource
	 * @return string The URI
	 * @throws Exception
	 */
	public function getPublicPersistentResourceUri(Resource $resource) {
		if ($this->baseUri != '') {
			return $this->baseUri . $this->getRelativePublicationPathAndFilename($resource);
		} else {
			return $this->s3Client->getObjectUrl($this->bucketName, $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource));
		}
	}

	/**
	 * Publishes the specified source file to this target, with the given relative path.
	 *
	 * @param resource $sourceStream
	 * @param string $relativeTargetPathAndFilename
	 * @param ResourceMetaDataInterface $metaData
	 * @throws \Exception
	 */
	protected function publishFile($sourceStream, $relativeTargetPathAndFilename, ResourceMetaDataInterface $metaData) {
		$objectName = $this->keyPrefix . $relativeTargetPathAndFilename;
		$options = array(
			'ContentLength' => $metaData->getFileSize(),
			'ContentType' => $metaData->getMediaType()
		);

		try {
			$this->s3Client->upload($this->bucketName, $objectName, $sourceStream, 'public-read', $options);
			$this->systemLogger->log(sprintf('Successfully published resource as object "%s" in bucket "%s" with MD5 hash "%s"', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown'), LOG_DEBUG);
		} catch(\Exception $e) {
			$this->systemLogger->log(sprintf('Failed publishing resource as object "%s" in bucket "%s" with MD5 hash "%s": %s', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown', $e->getMessage()), LOG_DEBUG);
			if (is_resource($sourceStream)) {
				fclose($sourceStream);
			}
			throw $e;
		}
	}

	/**
	 * Determines and returns the relative path and filename for the given Storage Object or Resource. If the given
	 * object represents a persistent resource, its own relative publication path will be empty. If the given object
	 * represents a static resources, it will contain a relative path.
	 *
	 * @param ResourceMetaDataInterface $object Resource or Storage Object
	 * @return string The relative path and filename, for example "c828d0f88ce197be1aff7cc2e5e86b1244241ac6/MyPicture.jpg"
	 */
	protected function getRelativePublicationPathAndFilename(ResourceMetaDataInterface $object) {
		if ($object->getRelativePublicationPath() !== '') {
			$pathAndFilename = $object->getRelativePublicationPath() . $object->getFilename();
		} else {
			$pathAndFilename = $object->getSha1() . '/' . $object->getFilename();
		}
		return $pathAndFilename;
	}

}

?>
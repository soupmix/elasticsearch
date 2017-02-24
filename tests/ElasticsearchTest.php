<?php
namespace tests;

use Soupmix\ElasticSearch;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\NoNodesAvailableException;
use Elasticsearch\Common\Exceptions\Missing404Exception;

class ElasticsearchTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \Soupmix\ElasticSearch $client
     */
    protected $client = null;

    protected function setUp()
    {
        $config =[
            'db_name' => 'test',
            'hosts'   => ['127.0.0.1:9200'],
        ];

        $client = ClientBuilder::create()->setHosts($config['hosts'])->build();
        $this->client = new ElasticSearch($config, $client);
        $this->client->drop('test');
    }

    public function testInvalidConnection()
    {
        $this->expectException(NoNodesAvailableException::class);
        $config =[
            'db_name' => 'test1',
            'hosts'   => [['host' => '127.0.0.1', 'port' => 5200]],
        ];

        $client = ClientBuilder::create()->setHosts($config['hosts'])->build();
        $esClient = new ElasticSearch($config, $client);
    }

    public function testSettingHostsAsArray()
    {
        $config =[
            'db_name' => 'test2',
            'hosts'   => [['host' => '127.0.0.1', 'port' => 9200]],
        ];

        $client = ClientBuilder::create()->setHosts($config['hosts'])->build();
        $esClient = new ElasticSearch($config, $client);

        $docId = $esClient->insert('test', ['id' => 1, 'title' => 'test']);
        $document = $esClient->get('test', $docId);
        $this->assertArrayHasKey('title', $document);
        $this->assertArrayHasKey('id', $document);
        $result = $esClient->delete('test', ['_id' => $docId]);
        $this->assertTrue($result == 1);
        $this->client->drop('test2');
    }

    public function testInvalidDocumentIndex()
    {
        $docId = $this->client->insert('test', null);
        $this->assertNull($docId);
        $docId = $this->client->insert('test', ['_id' => 1]);
        $this->assertNull($docId);
    }

    public function testInsertGetDocument()
    {
        $docId = $this->client->insert('test', ['id' => 1, 'title' => 'test']);
        $document = $this->client->get('test', $docId);
        $this->assertArrayHasKey('title', $document);
        $this->assertArrayHasKey('id', $document);
        $result = $this->client->delete('test', ['_id' => $docId]);
        $this->assertTrue($result == 1);
    }

    public function testGetInvalidDocument()
    {
        $document = $this->client->get('test', 122333232323);
        $this->assertNull($document);
        $document = $this->client->get('test', [122333232323, 123344444]);
        $this->assertNull($document);
    }

    public function testFindDocumentsWithSort()
    {
        $this->populateBulkData('test');
        $results = $this->client->find('test', ['balance__gte' => 100], null, ['id' => 'asc']);
        $this->assertEquals($results['data'][0]['id'], 1);
    }

    public function testFindDocumentsWithMultipleSort()
    {
        $this->populateBulkData('test');
        $results = $this->client->find('test', ['balance__gte' => 100], null, ['balance' => 'asc', 'id' => 'desc']);
        $this->assertEquals($results['data'][0]['id'], 3);
    }

    public function testFindDocuments()
    {
        $docIds = [];
        $this->populateBulkData('test');
        $this->client->getConnection()->indices()->refresh([]); // waiting to be able to be searchable on elasticsearch.
        $results = $this->client->find('test', ['title' => 'test1']);
        $this->assertArrayHasKey('total', $results);
        $this->assertArrayHasKey('data', $results);
        $this->assertGreaterThanOrEqual(1, $results['total'], 'Total not equal for field term filtering');

        $results = $this->client->find('test', ['count.max__gte' => 6]);
        $this->assertGreaterThanOrEqual(2, $results['total'],
            'Total not greater than or equal to 2 on count_gte filtering');

        $results = $this->client->find('test', ['count.max__gte' => 6, 'count.min__gte' => 2]);
        $this->assertGreaterThanOrEqual(1, $results['total'],
            'Total not greater than or equal to 2 on count.max__gte and count.max__gte filtering');
        $results = $this->client->find('test', ['count.max__gte' => 2],['title', 'balance']);
        $this->assertArrayHasKey('title', $results['data'][0]);
        $this->assertArrayNotHasKey('count', $results['data'][0]);
        $results = $this->client->find('test', [[['count.max__gte' => 6], ['count.min__gte' => 2]], 'title' => 'test4']);
        $this->assertGreaterThanOrEqual(1, $results['total'],
            'Total not greater than or equal to 2 on count.max__gte and count.max__gte filtering');

        foreach ($docIds as $docId) {
            $result = $this->client->delete('test', ['_id' => $docId]);
            $this->assertTrue($result == 1);
        }
    }

    public function testInsertUpdateGetDocument()
    {
        $docId = $this->client->insert('test', ['id' => 1, 'title' => 'test']);
        $this->client->getConnection()->indices()->refresh([]); // waiting to be able to be searchable on elasticsearch.
        $modifiedCount = $this->client->update('test', ['title' => 'test'], ['title' => 'test2']);
        $this->assertTrue($modifiedCount >= 1);
        $this->client->getConnection()->indices()->refresh([]); // waiting to be able to be searchable on elasticsearch.
        $document = $this->client->get('test', $docId);
        $this->assertArrayHasKey('title', $document);
        $this->assertEquals('test2', $document['title']);

        $result = $this->client->delete('test', ['_id' => $docId]);
        $this->assertTrue($result == 1);
    }

    public function testInsertUpdateMultipleDocument()
    {
        $docIds = array();
        $docIds[] = $this->client->insert('test', ['id' => 1, 'title' => 'test']);
        $docIds[] = $this->client->insert('test', ['id' => 2, 'title' => 'test']);
        $this->client->getConnection()->indices()->refresh([]); // waiting to be able to be searchable on elasticsearch.
        $modifiedCount = $this->client->update('test', ['title' => 'test'], ['title' => 'test_2']);
        $this->assertTrue($modifiedCount >= 2);
        $this->client->getConnection()->indices()->refresh([]); // waiting to be able to be searchable on elasticsearch.
        //Testing get multiple ids
        $documents = $this->client->get('test', $docIds);
        foreach ($documents as $document) {
            $this->assertArrayHasKey('title', $document);
            $this->assertEquals('test_2', $document['title']);
        }
        $result = $this->client->delete('test', ['title' => 'test_2']);
        $this->assertEquals(2, $result);
    }

    public function testUpdateInvalidDocument()
    {
        // In fact, we expect return 0 as a result
        // if ($docs['total']===0) {
        //     return 0;
        // }

        $this->expectException(Missing404Exception::class);
        $modifiedCount = $this->client->update('test', ['title' => 'invalid_title_value'], ['title' => 'test_2']);
    }

    public function testDeleteInvalidDocument()
    {
        $deletedCount = $this->client->delete('test', ['_id' => 'invalid_title_value']);
        $this->assertEquals($deletedCount, 0);
        $deletedCount = $this->client->delete('test', ['title' => 'invalid_title_value']);
        $this->assertEquals($deletedCount, 0);
    }

    public function testInvalidIndexNameForFindMethod()
    {
        $this->expectException(Missing404Exception::class);
        $results = $this->client->find('test', ['balance.max__gte' => 600]);
    }

    public function testNoResultFindMethod()
    {
        $data = $this->populateBulkData('test');
        $results = $this->client->find('test', ['id' => 343344]);
        $this->assertArrayHasKey('total', $results);
        $this->assertArrayHasKey('data', $results);
        $this->assertEquals(0, $results['total']);
        $this->assertNull($results['data']);
    }

    public function testOneResultFindMethod()
    {
        $data = $this->populateBulkData('test');
        $results = $this->client->find('test', ['id' => 1]);
        $this->assertArrayHasKey('total', $results);
        $this->assertArrayHasKey('data', $results);
        $this->assertEquals(1, $results['total']);
    }

    public function testTruncateMethod()
    {
        $this->client->truncate('test');
    }

    public function testCreateMethod()
    {
        $this->client->create('test', ['id', 'date', 'title', 'balance', 'count']);
    }

    public function testCreateIndexesMethod()
    {
        $this->client->createIndexes('test', ['index1', 'index2']);
    }

    protected function tearDown()
    {
        $this->client->drop('test');
    }

    private function populateBulkData($collection)
    {
        $data = $this->bulkData();
        foreach ($data as $d) {
            $docId = $this->client->insert($collection, $d);
            if ($docId) {
                $docIds[] = $docId;
            }
        }
        sleep(1); // waiting for elasticsearch indexing process
    }

    private function bulkData()
    {
        return [
            ['id' => 1, 'date' => '2015-04-10 00:00:00', 'title' => 'test1', 'balance' => 100.0, 'count' => ['min' => 1, 'max' => 1]],
            ['id' => 2, 'date' => '2015-04-11 00:00:00', 'title' => 'test2', 'balance' => 120.0, 'count' =>  ['min' => 1, 'max' => 1]],
            ['id' => 3, 'date' => '2015-04-12 00:00:00', 'title' => 'test3', 'balance' => 100.0, 'count' =>  ['min' => 1, 'max' => 7]],
            ['id' => 4, 'date' => '2015-04-12 00:00:00', 'title' => 'test4', 'balance' => 200.5, 'count' =>  ['min' => 3, 'max' => 6]],
            ['id' => 5, 'date' => '2015-04-13 00:00:00', 'title' => 'test5', 'balance' => 150.0, 'count' =>  ['min' => 1, 'max' => 5]],
            ['id' => 6, 'date' => '2015-04-14 00:00:00', 'title' => 'test6', 'balance' => 400.8, 'count' =>  ['min' => 1, 'max' => 4]],
            ['id' => 7, 'date' => '2015-04-15 00:00:00', 'title' => 'test7', 'balance' => 240.0, 'count' =>  ['min' => 1, 'max' => 4]],
            ['id' => 8, 'date' => '2015-04-20 00:00:00', 'title' => 'test8', 'balance' => 760.0, 'count' =>  ['min' => 1, 'max' => 5]],
            ['id' => 9, 'date' => '2015-04-20 00:00:00', 'title' => 'test9', 'balance' => 50.0, 'count' =>  ['min' => 1, 'max' => 2]],
            ['id' => 10, 'date' => '2015-04-21 00:00:00', 'title' => 'test0', 'balance' => 55.5, 'count' =>  ['min' => 1, 'max' => 2]],
        ];
    }
}

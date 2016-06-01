<?php
namespace tests;

use Soupmix\ElasticSearch;

class ElasticsearchTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \Soupmix\ElasticSearch $client
     */
    protected $client = null;

    protected function setUp()
    {
        $this->client = new ElasticSearch([
            'db_name' => 'test',
            'hosts'   => ['127.0.0.1:9200'],
        ]);
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

    public function testFindDocuments()
    {
        $docId1 = $this->client->insert('test', ['id' => 1, 'title' => 'test']);
        sleep(1); // waiting to be able to be searchable on elasticsearch.
        $results = $this->client->find('test', ['title' => 'test']);
        $this->assertArrayHasKey('total', $results);
        $this->assertArrayHasKey('data', $results);
        $this->assertCount($results['total'], $results['data']);

        $result = $this->client->delete('test', ['_id' => $docId1]);
        $this->assertTrue($result == 1);
    }

    public function testInsertUpdateGetDocument()
    {
//        $docId = $this->client->insert('test', ['id' => 1, 'title' => 'test']);
//        sleep(1); // waiting to be able to be searchable on elasticsearch.
//        $modifiedCount = $this->client->update('test', ['title' => 'test'], ['title' => 'test2']);
//        $this->assertTrue($modifiedCount >= 1);
//        sleep(1); // waiting to be able to be searchable on elasticsearch.
//        $document = $this->client->get('test', $docId);
//        $this->assertArrayHasKey('title', $document);
//        $this->assertEquals('test2', $document['title']);
//
//        $result = $this->client->delete('test', ['_id' => $docId]);
//        $this->assertTrue($result == 1);
    }
}

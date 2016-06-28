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
        $this->client->drop('test');
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
        $docIds = [];
        $data = $this->bulkData();
        foreach ($data as $d) {
            $docId = $this->client->insert('test', $d);
            $this->assertNotNull($docId, 'Document could not inserted to ES while testing find');
            if ($docId) {
                $docIds[] = $docId;
            }
        }
        sleep(1); // waiting to be able to be searchable on elasticsearch.
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

    public function bulkData()
    {
        return [
            ['id' => 1, 'date' => '2015-04-10 00:00:00', 'title' => 'test1', 'balance' => 100.0, 'count' => ['min' => 1, 'max' => 1]],
            ['id' => 2, 'date' => '2015-04-11 00:00:00', 'title' => 'test2', 'balance' => 120.0, 'count' =>  ['min' => 1, 'max' => 1]],
            ['id' => 3, 'date' => '2015-04-12 00:00:00', 'title' => 'test3', 'balance' => 101.5, 'count' =>  ['min' => 1, 'max' => 7]],
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

<?php

namespace Soupmix;
/*
Elasticsearch Adapter
*/
Use Elasticsearch\Client;

final class ElasticSearch implements Base
{
    private $conn = null;
    private $index = null;
    private $esMajorVersion = 2;
    private static $filteredOrBool = [2 => 'filtered', 5 => 'bool', 6 => 'bool'];

    private static $operators = [
        'range'     => ['gt', 'gte', 'lt', 'lte'],
        'standart'  => ['prefix', 'regexp', 'wildcard'],
        'BooleanQueryLevel' => ['not'],
        'special'   => ['in']
    ];

    public function __construct($config, Client $client)
    {
        $this->index = $config['db_name'];
        $this->conn = $client;
        $this->esMajorVersion = substr($client->info()['version']['number'],0,1);
    }


    public function getConnection()
    {
        return $this->conn;
    }

    public function create(string $collection, array $fields)
    {
        $params = ['index' => $this->index];
        try {
            if (!$this->conn->indices()->exists($params)) {
                $this->conn->indices()->create($params);
            }
            $params['type'] = $collection;
            $params['body'] = [$collection => ['properties' => $fields]];
            $this->conn->indices()->putMapping($params);
        } catch (\Exception $e) {
            // This ignore the error
            return false;
        }
        return true;
    }

    public function drop(string $collection)
    {
        $params = ['index' => $this->index, 'type' => $collection];
        try {
            //$this->truncate($collection);
            //$mappingData = $this->conn->indices()->getMapping($params);
            //$mapping = $mappingData[$this->index]['mappings'][$collection];
            //$ma = $this->conn->indices()->deleteMapping($params);

        } catch (\Exception $e) {
            // This ignore the error
            return false;
        }
        return true;
    }

    public function truncate(string $collection)
    {
        $params = ['index' => $this->index, 'type' => $collection, 'body' => ['query' => ['bool' =>['match_all' => []]]]];
        $this->conn->deleteByQuery($params);
    }

    public function createIndexes(string $collection, array $indexes)
    {}

    public function insert(string $collection, array $values)
    {
        $params = [];
        $params['body'] = $values;
        $params['index'] = $this->index;
        $params['type'] = $collection;
        try {
            $result = $this->conn->index($params);
        } catch (\Exception $e) {
            return;
        }
        if (
            array_key_exists('created', $result)
            || (array_key_exists('result', $result) && $result['result'] === 'created')
        ) {
            return $result['_id'];
        }
        return null;
    }

    public function get(string $collection, $docId)
    {
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;

        try {
            if (is_array($docId)) {
                $params['body'] = [
                    'query' => [
                        self::$filteredOrBool[$this->esMajorVersion] => [
                            'filter' => [
                                'ids' => ['values' => $docId]
                            ],
                        ],
                    ],
                ];
                $results = $this->conn->search($params);
                if ($results['hits']['total'] === 0) {
                    return;
                }
                $result = [];
                foreach ($results['hits']['hits'] as $item){
                    $result[$item['_id']]=$item['_source'];
                }
                return $result;
            } else {
                $params['id'] = $docId;
                $result = $this->conn->get($params);
            }
            if ($result['found']) {
                $result['_source']['_id'] = $result['_id'];
                return $result['_source'];
            } else {
                return;
            }
        } catch (\Exception $e) {
            return;
        }
    }

    public function update(string $collection, array $filter, array $values)
    {
        $docs = $this->find($collection, $filter, ['_id']);
        if ($docs['total'] ===0) {
            return 0;
        }
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;
        $modified_count = 0;
        foreach ($docs['data'] as $doc) {
            $params['id'] = $doc['_id'];
            $params['body']['doc'] = $values;
            try {
                $this->conn->update($params);
                ++$modified_count;
            } catch (\Exception $e) {
                // throw new \Exception($e->getMessage());
            }
        }

        return $modified_count;
    }

    public function delete(string $collection, array $filter)
    {
        if (isset($filter['_id'])) {
            $params = [];
            $params['index'] = $this->index;
            $params['type'] = $collection;
            $params['id'] = $filter['_id'];
            try {
                $result = $this->conn->delete($params);
            } catch (\Exception $e) {
                return 0;
            }
            if (
                array_key_exists('found', $result)
                || (array_key_exists('result', $result) && $result['result'] === 'deleted')
            ) {
                return 1;
            }
            return 0;
        }
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;
        $params['fields'] = '_id';
        $results = $this->find($collection, $filter, ['_id'], null, 0, 10000);
        $deleteCount = 0;
        if ($results['total']>0) {
            $dParams = [];
            $dParams['index'] = $this->index;
            $dParams['type'] = $collection;
            foreach ($results['data'] as $result) {
                $dParams['id'] = $result['_id'];
                try {
                    $result = $this->conn->delete($dParams);
                } catch (\Exception $e) {
                    return 0;
                }
                if (
                    array_key_exists('found', $result)
                    || (array_key_exists('result', $result) && $result['result'] === 'deleted')
                ) {
                    $deleteCount++;
                }
            }
            return $deleteCount;
        }
        return 0;
    }

    public function find(
        string $collection,
            ?array $filters,
            ?array $fields = null,
            ?array $sort = null,
            int $start = 0,
            int$limit = 25
    ) : array
    {
        $return_type = '_source';
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;
        $params['body'] = [];
        if ($filters!==null) {
            $filters = self::buildFilter($filters);
            $params['body'] = [
                'query' => [
                    self::$filteredOrBool[$this->esMajorVersion] => [
                        'filter' => [
                            'bool' => $filters,
                        ],
                    ],
                ],
            ];
        }
        $count = $this->conn->count($params);
        if ($sort !== null) {
            $params['body']['sort'] = [];
            foreach ($sort as $sort_key => $sort_dir) {
                $params['body']['sort'][] = [$sort_key => $sort_dir];
            }
        }
        if ($fields !== null) {
            $params['_source'] = $fields;
        }
        $params['from'] = (int) $start;
        $params['size'] = (int) $limit;

        $return = $this->conn->search($params);

        if ($return['hits']['total'] === 0) {
            return ['total' => 0, 'data' => null];
        }
        elseif ($limit === 1) {
            $return['hits']['hits'][0][$return_type]['_id'] = $return['hits']['hits'][0]['_id'];
            return ['total' => 1, 'data' => $return['hits']['hits'][0][$return_type]];
        }
        $result = [];
        foreach ($return['hits']['hits'] as $item) {

            if (($return_type === 'fields') && ($fields !== ['_id'])) {
                foreach ($item[$return_type]as $k => $v) {
                    $item[$return_type][$k] = $v[0];
                }
            }
            $item[$return_type]['_id'] = $item['_id'];
            $result[] = $item[$return_type];
        }
        return ['total' => $count['count'], 'data' => $result];
    }

    public function query(string $collection)
    {
        return new ElasticSearchQueryBuilder($collection);
    }

    public static function buildFilter(array $filter)
    {
        $filters = [];
        foreach ($filter as $key => $value) {
            $isNot = '';
            if (strpos($key, '__')!==false) {
                $tmpFilters = self::buildFilterForKeys($key, $value, $isNot);
                $filters = self::mergeFilters($filters, $tmpFilters);
            } elseif ((strpos($key, '__') === false) && is_array($value)) {
                $filters['should'] = self::buildFilterForOR($value);
            } else {
                $filters['must'][] = ['term' => [$key => $value]];
            }
        }
        return $filters;
    }

    private static function mergeFilters (array $filters, array $tmpFilters){
        foreach ($tmpFilters as $fKey => $fVals) {
            if (isset($filters[$fKey])) {
                foreach ($fVals as $fVal) {
                    $filters[$fKey][] = $fVal;
                }
            } else {
                $filters[$fKey] = $fVals;
            }
        }
        return $filters;
    }

    private static function buildFilterForKeys(string $key, $value, string $isNot)
    {
        $filters = [];
        preg_match('/__(.*?)$/', $key, $matches);
        $operator = $matches[1];
        if (strpos($operator, '!')===0) {
            $operator = str_replace('!', '', $operator);
            $isNot = '_not';
        }
        $key = str_replace($matches[0], '', $key);
        foreach (self::$operators as $type => $possibilities) {
            if (in_array($operator, $possibilities, true)) {
                switch ($type) {
                    case 'range':
                        $filters['must'.$isNot][] = ['range' => [$key => [$operator => $value]]];
                        break;
                    case 'standard':
                        $filters['must'.$isNot][] = [$type => [$key => $value]];
                        break;
                    case 'BooleanQueryLevel':
                        switch ($operator) {
                            case 'not':
                                $filters['must_not'][] = ['term' => [$key => $value]];
                                break;
                        }
                        break;
                    case 'special':
                        switch ($operator) {
                            case 'in':
                                $filters['must'.$isNot][] = ['terms' => [$key => $value]];
                                break;
                        }
                        break;
                }
            }
        }
        return $filters;
    }

    private static function buildFilterForOR(array $orValues)
    {
        $filters = [];
        foreach ($orValues as $orValue) {
            $subKey = array_keys($orValue)[0];
            $subValue = $orValue[$subKey];
            if (strpos($subKey, '__') !== false) {
                preg_match('/__(.*?)$/', $subKey, $subMatches);
                $subOperator = $subMatches[1];
                if (strpos($subOperator, '!')===0) {
                    $subOperator = str_replace('!', '', $subOperator);
                }
                $subKey = str_replace($subMatches[0], '', $subKey);
                foreach (self::$operators as $type => $possibilities) {
                    if (in_array($subOperator, $possibilities, true)) {
                        switch ($type) {
                            case 'range':
                                $filters[] = ['range' => [$subKey => [$subOperator => $subValue]]];
                                break;
                            case 'standard':
                                $filters[] = [$type => [$subKey => $subValue]];
                                break;
                            case 'special':
                                switch ($subOperator) {
                                    case 'in':
                                        $filters[] = ['terms' => [$subKey => $subValue]];
                                        break;
                                }
                                break;
                        }
                    }
                }
            } else {
                $filters[] = ['term' => [$subKey => $subValue]];
            }
        }
        return $filters;
    }
}

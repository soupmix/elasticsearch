<?php

namespace Soupmix;
/*
Elasticsearch Adapter
*/
Use Elasticsearch as ElasticsearchLib;

class ElasticSearch implements Base
{
    public $conn = null;
    private $index = null;

    public function __construct($config)
    {
        $this->index = $config['db_name'];
        $this->connect($config);
    }

    public function connect($config)
    {
        $this->conn = ElasticsearchLib\ClientBuilder::create()->setHosts($config['hosts'])->build();
    }

    public function create($collection)
    {
    }

    public function drop($collection)
    {
        $params = ['index' => $this->index];
        try {
            $this->conn->indices()->delete($params);
        } catch (\Exception $e) {
            // This ignore the error
            return false;
        }
        return true;
    }

    public function truncate($collection)
    {
    }

    public function createIndexes($collection, $indexes)
    {
    }

    public function insert($collection, $values)
    {
        $params = [];
        $params['body'] = $values;
        $params['index'] = $this->index;
        $params['type'] = $collection;
        try {
            $result = $this->conn->index($params);
            if ($result['created']) {
                return $result['_id'];
            }
            return null;
        } catch (\Exception $e) {
            return;
        }
    }

    public function get($collection, $docId)
    {
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;

        try {
            if (gettype($docId) == "array") {
                $params['body'] = [
                    'query' => [
                        'filtered' => [
                            'filter' => [
                                'ids' => ['values'=>$docId]
                            ],
                        ],
                    ],
                ];
                $results = $this->conn->search($params);
                if ($results['hits']['total'] == 0) {
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

    public function update($collection, $filter, $values)
    {
        $docs = $this->find($collection, $filter, ['_id']);
        if ($docs['total']===0) {
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
                $return = $this->conn->update($params);
                if ($return['_shards']['successful']==1) {
                    ++$modified_count;
                }
            } catch (\Exception $e) {
                // should we throw exception? Probably not.
            }
        }

        return $modified_count;
    }

    public function delete($collection, $filter)
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
            if ($result['found']) {
                return 1;
            }
        } else {
            $params = [];
            $params['index'] = $this->index;
            $params['type'] = $collection;
            $params['fields'] = '_id';
            $result = $this->find('users', $filter, ['_id'], null, 0, 1);
            if ($result['total']==1) {
                $params = [];
                $params['index'] = $this->index;
                $params['type'] = $collection;
                $params['id'] = $result['data']['_id'];
                try {
                    $result = $this->conn->delete($params);
                } catch (\Exception $e) {
                    return 0;
                }
                if ($result['found']) {
                    return 1;
                }
            }
        }

        return 0;
    }

    public function find($collection, $filters, $fields = null, $sort = null, $start = 0, $limit = 25, $debug = false)
    {
        $return_type = '_source';
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;
        if ($filters!==null) {
            $filters = self::buildFilter($filters);
            $params['body'] = [
                'query' => [
                    'filtered' => [
                        'filter' => [
                            'bool' => $filters,
                        ],
                    ],
                ],
            ];
        }
        $count = $this->conn->count($params);
        if ($fields!==null) {
            $params['fields'] = implode(',', $fields);
            $return_type = 'fields';
        }
        if ($sort!==null) {
            $params['sort'] = '';
            foreach ($sort as $sort_key => $sort_dir) {
                if ($params['sort']!='') {
                    $params['sort'] .= ',';
                }
                $params['sort'] .= $sort_key.':'.$sort_dir;
            }
        }
        if ($fields!='') {
            $params['fields'] = $fields;
            $return_type = 'fields';
        }
        $params['from'] = (int) $start;
        $params['size'] = (int) $limit;
        if ($debug) {
           return $params;
        }
        $return = $this->conn->search($params);
        if ($return['hits']['total']==0) {
            return ['total' => 0, 'data' => null];
        }
        elseif ($limit==1) {
            $return['hits']['hits'][0][$return_type]['_id'] = $return['hits']['hits'][0]['_id'];
            return ['total' => 1, 'data' => $return['hits']['hits'][0][$return_type]];
        }
        $result = array();
        foreach ($return['hits']['hits'] as $item) {
            if ($return_type=='fields') {
                foreach ($item[$return_type]as $k => $v) {
                    $item[$return_type][$k] = $v[0];
                }
            }
            $item[$return_type]['_id'] = $item['_id'];
            $result[] = $item[$return_type];
        }
        return ['total' => $count['count'], 'data' => $result];
    }

    public function query($query)
    {
        // reserved        
    }

    public static function buildFilter($filter)
    {

        $operators = [];
        $operators['range'] = ['gt', 'gte', 'lt', 'lte'];
        $operators['standart'] = ['prefix', 'regexp', 'wildchard'];
        $operators['BooleanQueryLevel'] = ['not'];
        $operators['special'] = ['in'];

        $filters = [];
        foreach ($filter as $key => $value) {
            $is_not = '';
            if (strpos($key, '__')!==false) {
                preg_match('/__(.*?)$/i', $key, $matches);
                $operator = $matches[1];
                if (strpos($operator, '!')===0) {
                    $operator = str_replace('!', '', $operator);
                    $is_not = '_not';
                }
                $key = str_replace($matches[0], '', $key);
                foreach ($operators as $type => $possibilities) {
                    if (in_array($operator, $possibilities)) {
                        switch ($type) {
                            case 'range':
                                $filters['must'.$is_not][] = ['range' => [$key => [$operator => $value]]];
                                break;
                            case 'standart':
                                $filters['must'.$is_not][] = [$type => [$key => $value]];
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
                                        $filters['must'.$is_not][] = ['terms' => [$key => $value]];
                                        break;
                                }
                                break;
                        }
                    }
                }
            } elseif (strpos($key, '__')===false && is_array($value)) {
                foreach ($value as $skey => $svalue) {
                    if (strpos($skey, '__')!==false) {
                        preg_match('/__(.*?)$/i', $skey, $smatches);
                        $soperator = $smatches[1];
                        if (strpos($soperator, '!')===0) {
                            $soperator = str_replace('!', '', $soperator);
                        }
                        $skey = str_replace($smatches[0], '', $skey);
                        foreach ($operators as $type => $possibilities) {
                            if (in_array($soperator, $possibilities)) {
                                switch ($type) {
                                    case 'range':
                                        $filters['should'][] = ['range' => [$skey => [$soperator => $svalue]]];
                                        break;
                                    case 'standart':
                                        $filters['should'][] = [$type => [$skey => $svalue]];
                                        break;
                                    case 'special':
                                        switch ($soperator) {
                                            case 'in':
                                                $filters['should'][] = ['terms' => [$skey => $svalue]];
                                                break;
                                        }
                                        break;
                                }
                            }
                        }
                    } else {
                        $filters['should'][] = ['term' => [$skey => $svalue]];
                    }
                }
            } else {
                $filters['must'][] = ['term' => [$key => $value]];
            }
        }
        return $filters;
    }
}

from elasticsearch import Elasticsearch

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

es_client = Elasticsearch('http://elasticsearch:9200') 
def elastic_search(query, index_name, es_client=es_client):
    search_query = {
        "size": 5,
        "query": {
            "multi_match": {
                    "query": query,
                        "fields": ["question^3", "text", "section"],
                        "type": "best_fields"
            }    
        }
    }
    response = es_client.search(index=index_name, body=search_query)
    
    result_docs = []
    
    for hit in response['hits']['hits']:
        result_docs.append(hit['_source'])
    
    return result_docs

    
    

@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here

    query = "When is the next cohort?"

    index_name = kwargs['index_name']
    print(index_name)
    result = elastic_search(query, index_name)
    print(result)

    return result


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

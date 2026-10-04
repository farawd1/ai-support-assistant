import asyncio
import json
import pytest
from fastapi.testclient import TestClient
from app import main, providers

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(main, 'DB', str(tmp_path / 'test.db'))
    monkeypatch.setattr(main, 'MODE', 'demo')
    with TestClient(main.app) as c:
        yield c

def test_end_to_end_and_edit_metrics(client):
    r = client.post('/api/tickets', json={'text': 'С карты дважды списали оплату за заказ'})
    assert r.status_code == 200
    t = r.json()
    assert t['state'] == 'ready'
    assert t['result']['priority'] == 'P1'
    assert t['result']['draft']['citations']
    assert client.post(f"/api/tickets/{t['id']}/action", json={'action':'accepted','reply':'Изменённый ответ'}).json()['action'] == 'edited'
    metrics = client.get('/api/analytics').json()
    assert metrics['reviewed'] == 1 and metrics['acceptance_rate'] == 0
    client.post(f"/api/tickets/{t['id']}/action", json={'action':'accepted','reply':t['result']['draft']['reply']})
    assert client.get('/api/analytics').json()['reviewed'] == 1

def test_unknown_and_security(client):
    unknown = client.post('/api/tickets', json={'text':'Телепортация в соседнюю галактику'}).json()
    assert unknown['result']['draft']['status'] == 'clarify'
    security = client.post('/api/tickets', json={'text':'Мой аккаунт взломали'}).json()
    assert security['result']['priority'] == 'P0'
    assert security['result']['draft']['status'] == 'escalate'

def test_new_knowledge_is_searchable(client):
    r = client.post('/api/documents', json={'title':'Промокод ORBIT42','topic':'information','body':'Промокод ORBIT42 действует только на первый заказ.'})
    assert r.status_code == 200
    t = client.post('/api/tickets', json={'text':'Какие условия промокода ORBIT42?'}).json()
    assert r.json()['id'] in [d['id'] for d in t['result']['documents']]

def test_citation_rejection():
    d = providers.Draft(status='draft', reply='Ответ', citations=[{'document_id':1,'quote':'выдумка'}], operator_note='')
    with pytest.raises(ValueError):
        providers.validate_citations(d, [{'id':1,'body':'настоящая цитата'}])
    d.citations = []
    with pytest.raises(ValueError):
        providers.validate_citations(d, [])

def test_live_contracts(monkeypatch):
    calls = []
    monkeypatch.setenv('TYPESAFE_API_KEY','test')
    monkeypatch.setenv('DEEPSEEK_API_KEY','test')
    async def fake(url, key, body):
        calls.append(body)
        if 'typesafe' in url:
            return {'model':'jev-1.13.0','answers':{'topic':{'choice':'payment','confidence':.9},'security':{'noul':.1},'money':{'noul':.9},'blocked':{'noul':.1}}}
        return {'model':'deepseek-flash','choices':[{'message':{'content':json.dumps({'status':'draft','reply':'Ответ','citations':[{'document_id':1,'quote':'Проверьте оплату'}],'operator_note':''})}}]}
    monkeypatch.setattr(providers,'post',fake)
    c, _ = asyncio.run(providers.classify('оплата', 'live'))
    assert providers.priority(c)[0] == 'P1'
    d, _ = asyncio.run(providers.generate('оплата',[{'id':1,'body':'Проверьте оплату'}],'live','P1'))
    assert d.citations[0].document_id == 1
    assert calls[0]['questions']['topic']['type'] == 'choice'
    assert calls[1]['response_format']['type'] == 'json_object'

def test_failure_visible(client, monkeypatch):
    async def fail(*args):
        raise RuntimeError('secret must not leak')
    monkeypatch.setattr(main,'classify',fail)
    t = client.post('/api/tickets',json={'text':'Проблема с оплатой'}).json()
    assert t['state'] == 'failed' and 'secret' not in t['error']
    assert client.get('/api/analytics').json()['failed'] == 1

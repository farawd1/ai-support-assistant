"""Vercel's native, private queue subscriber. Payloads contain IDs only."""
import asyncio
import time
from uuid import uuid4
from vercel.queue import subscribe
from app import main

@subscribe(topic='repl-support',max_concurrency=4,max_attempts=10)
async def support_task(task):
    kind=task['kind']
    if kind=='analyze':
        ticket_id=task['ticket_id']
        token=uuid4().hex
        with main.db() as c:
            c.execute('BEGIN IMMEDIATE')
            row=c.execute('SELECT state,processing_started FROM tickets WHERE id=?',(ticket_id,)).fetchone()
            if row is None or row['state'] in ('ready','failed'): return
            if row['state']=='processing' and (row['processing_started'] or 0)>time.time()-360:
                raise RuntimeError('Previous processing lease is still active')
            c.execute("UPDATE tickets SET state='processing',processing_started=?,processing_token=? WHERE id=?",(time.time(),token,ticket_id))
        try:
            await asyncio.wait_for(main.analyze(ticket_id,processing_token=token),240)
        except TimeoutError:
            with main.db() as c:
                c.execute("UPDATE tickets SET state='failed',error='AI-анализ превысил лимит времени. Повторите анализ.' WHERE id=? AND processing_token=?",(ticket_id,token))
        await main.enqueue_work('autopilot')
    elif kind=='autopilot':
        with main.db() as c:
            c.execute("UPDATE tickets SET auto_state='pending' WHERE auto_state='processing' AND auto_started<?",(time.time()-360,))
        await asyncio.wait_for(main.auto_step(),240)
        with main.db() as c:
            settings=main.autopilot_settings(c)
            pending=c.execute("SELECT 1 FROM tickets WHERE state='ready' AND action IS NULL AND auto_state='pending' LIMIT 1").fetchone()
        if settings['enabled'] and pending: await main.enqueue_work('autopilot')
    else:
        raise ValueError('Unknown queue task')

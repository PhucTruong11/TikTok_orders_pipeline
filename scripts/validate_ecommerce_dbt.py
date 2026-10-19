"""Validate two-source medallion models and regression cases in isolated databases."""
import argparse
import csv
from decimal import Decimal
import json
from pathlib import Path
import shutil
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import duckdb
from pipeline_orchestration.warehouse import (PROJECT, run_dbt, verify_database, fingerprint,
    publish_database, build_and_publish, source_paths)


def records(path):
    with path.open(encoding="utf-8", newline="") as stream:
        reader=csv.DictReader(stream)
        return reader.fieldnames,list(reader)


def write_records(path, fields, rows):
    with path.open("w",encoding="utf-8",newline="") as stream:
        writer=csv.DictWriter(stream,fieldnames=fields)
        writer.writeheader();writer.writerows(rows)


def snapshot(database):
    with duckdb.connect(str(database),read_only=True) as connection:
        return connection.execute("""
          select observation_id,source_channel,_source_row_number,standardized_status,
                 is_completed,is_cancelled,is_returned,is_return_requested
          from silver.silver_ecommerce_observations order by source_channel,_source_row_number
        """).fetchall()


def verify_fixture(database,sources):
    result=verify_database(database,sources)
    assert result['tiktok']['observations']==8 and result['shopee']['observations']==7
    rows=snapshot(database)
    assert len(rows)==15 and len({row[0] for row in rows})==15
    for ch,count in [('tiktok',8),('shopee',7)]:
        assert [row[2] for row in rows if row[1]==ch]==list(range(1,count+1))
    tt=[r for r in rows if r[1]=='tiktok'];sp=[r for r in rows if r[1]=='shopee']
    assert [sum(r[i] for r in tt) for i in (4,5,6,7)]==[3,1,1,0]
    assert [sum(r[i] for r in sp) for i in (4,5,6,7)]==[3,1,0,1], 'Return request was incorrectly classified as completed return'
    with duckdb.connect(str(database),read_only=True) as connection:
        assert connection.execute("select count(*) from silver.silver_shopee_observations where item_sale_price is null").fetchone()[0]==1
        assert connection.execute("select count(*) from silver.silver_shopee_observations where escrow_amount<0").fetchone()[0]>0
        assert connection.execute("select count(*) from silver.silver_shopee_observations where delivered_time is not null or delivery_sla_time is not null").fetchone()[0]==0
        assert connection.execute("select sum(observation_count) from gold.gold_observation_quality").fetchone()[0]==15
        for ch,path in sources.items():
            fields,raw=records(path)
            types=dict(connection.execute("select column_name,data_type from information_schema.columns where table_schema='bronze' and table_name=?",[f'bronze_{ch}_orders']).fetchall())
            assert all(types[field]=='VARCHAR' for field in fields), 'Bronze transformed a raw source type'
            expected={}
            price_col='item_sale_price' if ch=='tiktok' else 'model_discounted_price'
            for row in raw:
                if row[price_col]:
                    value,units=expected.get(row['currency'],(Decimal(0),0))
                    qty=int(row['quantity']);expected[row['currency']]=(value+Decimal(row[price_col])*qty,units+qty)
            for currency,(value,units) in expected.items():
                actual,priced_units=connection.execute("select sum(observed_sales_value_sum),sum(priced_unit_count) from gold.gold_channel_daily where source_channel=? and currency=?",[ch,currency]).fetchone()
                assert actual==value and priced_units==units, 'Missing prices or currency were aggregated incorrectly'
    return rows


def validate(real_data=False):
    validation_root=(ROOT/'.validation').resolve();validation_root.mkdir(exist_ok=True)
    assert validation_root.is_relative_to(ROOT.resolve())
    work=Path(tempfile.mkdtemp(prefix='ecommerce-',dir=validation_root))
    project=work/'dbt_transforms'
    shutil.copytree(PROJECT,project,ignore=shutil.ignore_patterns('target','logs','dbt_packages','.user.yml'))
    sources={}
    for ch in ['tiktok','shopee']:
        sources[ch]=work/f'{ch}.csv'
        shutil.copyfile(ROOT/f'tests/fixtures/{ch}_orders.csv',sources[ch])
    db=work/'ecom_medallion.duckdb';artifacts=work/'fixture-artifacts'
    build=run_dbt(project,sources,db,artifacts)
    first=verify_fixture(db,sources)
    run_dbt(project,sources,db,artifacts)
    assert verify_fixture(db,sources)==first,'Identical rerun changed observation identity/status'
    results=[{'case':'two_source_fixture_and_rerun','status':'passed','observations':15,'dbt_tests':sum(r['unique_id'].startswith('test.') for r in build['results'])}]
    print(json.dumps(results[-1]),flush=True)

    for name,ch,column,value,failure_node in [
        ('unknown_shopee_status','shopee','order_status','UNRECOGNIZED','not_null_silver_shopee_observations_standardized_status'),
        ('invalid_shopee_quantity','shopee','quantity','0','positive_value_silver_shopee_observations_quantity'),
        ('invalid_shopee_money','shopee','buyer_total_amount','not-a-number','silver_shopee_observations'),
        ('unknown_tiktok_status','tiktok','order_status','UNRECOGNIZED','not_null_silver_tiktok_observations_standardized_status')]:
        fields,raw=records(sources[ch]);raw[0][column]=value
        bad=work/f'{name}.csv';write_records(bad,fields,raw)
        bad_sources=dict(sources);bad_sources[ch]=bad
        bad_build=run_dbt(project,bad_sources,work/f'{name}.duckdb',work/name,expect_success=False)
        assert any(failure_node in r['unique_id'] and r['status'] in ('fail','error') for r in bad_build['results']),f'Wrong rejection reason: {name}'
        results.append({'case':name,'status':'correctly_rejected'});print(json.dumps(results[-1]),flush=True)

    seed=project/'seeds/silver_order_status_mapping.csv';original=seed.read_bytes()
    ending = b'\r\n' if b'\r\n' in original else b'\n'
    seed.write_bytes(original.rstrip(b'\r\n') + ending
                     + b'shopee,COMPLETED,COMPLETED,COMPLETED,synthetic duplicate' + ending)
    bad_build=run_dbt(project,sources,work/'mapping.duckdb',work/'mapping',expect_success=False)
    assert any('status_mapping_unique' in r['unique_id'] and r['status']=='fail' for r in bad_build['results'])
    seed.write_bytes(original)
    results.append({'case':'mapping_fanout','status':'correctly_rejected'});print(json.dumps(results[-1]),flush=True)

    published=work/'published/ecom_medallion.duckdb';published.parent.mkdir()
    candidate=work/'candidate/ecom_medallion.duckdb';candidate.parent.mkdir();shutil.copyfile(db,candidate)
    publish_database(candidate,published);verify_fixture(published,sources)
    before=fingerprint(published)
    bad_sources=dict(sources);bad_sources['shopee']=work/'unknown_shopee_status.csv'
    try:
        build_and_publish(published,bad_sources)
    except RuntimeError:
        assert fingerprint(published)==before,'Failed build replaced the good warehouse'
    else:
        raise AssertionError('Bad build was published')
    results.append({'case':'atomic_publish_and_failure_preserves_previous','status':'passed'});print(json.dumps(results[-1]),flush=True)

    fields,raw=records(sources['shopee']);raw[0]['buyer_remark']='synthetic snapshot changed'
    write_records(sources['shopee'],fields,raw)
    run_dbt(project,sources,db,artifacts)
    changed=verify_fixture(db,sources)
    for ch in ['tiktok','shopee']:
        old_ids={r[0] for r in first if r[1]==ch};new_ids={r[0] for r in changed if r[1]==ch}
        assert (old_ids==new_ids) if ch=='tiktok' else old_ids.isdisjoint(new_ids)
    results.append({'case':'changed_shopee_snapshot_does_not_change_tiktok_identity','status':'passed'});print(json.dumps(results[-1]),flush=True)

    if real_data:
        real=source_paths();hashes={ch:fingerprint(path) for ch,path in real.items()}
        actual_db=work/'real/ecom_medallion.duckdb';actual_db.parent.mkdir()
        actual=run_dbt(project,real,actual_db,work/'real-artifacts')
        counts=verify_database(actual_db,real)
        assert hashes=={ch:fingerprint(path) for ch,path in real.items()}
        results.append({'case':'full_local_snapshots','status':'passed','sources':counts,'dbt_tests':sum(r['unique_id'].startswith('test.') for r in actual['results'])})
        print(json.dumps(results[-1]),flush=True)
    summary={'artifact_directory':str(work),'fixture_database':str(db),'fixture_sources':{ch:str(path) for ch,path in sources.items()},'results':results}
    (validation_root/'latest_validation.json').write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--real-data',action='store_true')
    args=parser.parse_args();validate(args.real_data)

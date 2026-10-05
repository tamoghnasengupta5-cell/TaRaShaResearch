#!/usr/bin/env python3
"""Apply an inspected Technology workbook manifest through Research helpers.

No scoring formula changes. The manifest contains workbook hashes and resolved
issuer IDs. A full before-image of every touched table is durably saved before
the first write. --rollback uses guarded row deltas, never a database-wide reset.
"""
from __future__ import annotations
import argparse, collections, hashlib, json, math, os, sqlite3, sys
from pathlib import Path
from datetime import datetime, timezone
from sqlalchemy import text, create_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.dialects.postgresql import insert as pg_insert

REPO=Path('/Users/tamoghna/Documents/TaRaShaResearch')
sys.path.insert(0,str(REPO))
import db_models as models
import core_backend as app
from db_session import DbCompat,SessionLocal,get_engine
from db_orm import RelativeValuationCategories,RelativeValuationSubcategories

BASE=['companies','company_groups','company_group_members','industry_betas',
      'relative_valuation_categories','relative_valuation_subcategories','relative_valuation_company_assignments']
DERIVED=['levered_beta_annual','cost_of_equity_annual','wacc_annual','roic_wacc_spread_annual']
INPUTS=['debt_equity_annual','marginal_corporate_tax_rates','implied_equity_risk_premium_usa',
        'risk_free_rates','country_risk_premium','total_debt_annual','market_capitalization_annual',
        'pre_tax_cost_of_debt_annual','roic_direct_upload_annual']
TABLES=BASE+DERIVED+INPUTS
MUTABLE=['relative_valuation_categories','relative_valuation_subcategories','company_groups',
         'industry_betas','company_group_members','relative_valuation_company_assignments']+DERIVED

def save(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w') as f:
        json.dump(data,f,indent=2,default=str);f.flush();os.fsync(f.fileno())
    return hashlib.sha256(path.read_bytes()).hexdigest()

def snapshot(conn,tables=TABLES):
    return {t:[dict(r) for r in conn.execute('SELECT * FROM '+t).mappings()] for t in tables}

def ordered(rows):
    return sorted((json.dumps(r,sort_keys=True,default=str) for r in rows))

def compare_derived_rows(actual,expected):
    key=lambda r:(r['company_id'],r['fiscal_year'])
    left={key(r):r for r in actual};right={key(r):r for r in expected}
    assert left.keys()==right.keys(),('Derived keys differ',len(left),len(right),list(left.keys()-right.keys())[:3],list(right.keys()-left.keys())[:3])
    max_delta=0.;different=0
    for k,row in right.items():
        for col,value in row.items():
            got=left[k][col]
            if got==value:continue
            assert isinstance(value,(int,float)) and isinstance(got,(int,float)) and math.isclose(value,got,rel_tol=1e-12,abs_tol=1e-12),('Derived value differs',k,col,value,got)
            max_delta=max(max_delta,abs(value-got));different+=1
    return {'round_trip_differences':different,'maximum_absolute_difference':max_delta}

def active_values(data):
    groups={r['id']:r['name'] for r in data['company_groups']}
    bs=collections.defaultdict(list)
    for r in data['industry_betas']:bs[r['user_industry_bucket']].append(r)
    out={}
    for r in sorted(data['company_group_members'],key=lambda r:r['group_id']):
        name=groups[r['group_id']];defs=bs[name]
        if defs:out[r['company_id']]=(name,*[sum(x[f] for x in defs)/len(defs) for f in ['unlevered_beta','cash_adjusted_beta']])
    return out

def apply_assignments(conn,plan):
    """Use scoped additions and the existing app helpers; never global replace."""
    session=conn.session
    categories={r.name:r.id for r in session.query(RelativeValuationCategories)}
    subcategories={(r.category_id,r.name):r.id for r in session.query(RelativeValuationSubcategories)}
    for category,subcategory in plan['categories']:
        if category not in categories:
            obj=RelativeValuationCategories(name=category);session.add(obj);session.flush();categories[category]=obj.id
        key=(categories[category],subcategory)
        if key not in subcategories:
            obj=RelativeValuationSubcategories(category_id=key[0],name=key[1]);session.add(obj);session.flush();subcategories[key]=obj.id
    ids={r['company_id'] for r in plan['rows'] if r['company_id']}
    old=[tuple(r) for r in conn.execute('SELECT company_id,subcategory_id FROM relative_valuation_company_assignments') if r[0] in ids]
    app.remove_relative_valuation_company_assignments(conn,old)
    grouped=collections.defaultdict(list)
    for row in plan['rows']:
        if row['company_id']:grouped[subcategories[(categories[row['category']],row['subcategory'])]].append(row['company_id'])
    for sub,company_ids in grouped.items():app.add_relative_valuation_company_assignments(conn,company_ids,sub)
    now=datetime.now(timezone.utc).isoformat()
    existing={r['user_industry_bucket']:r for r in snapshot(conn,['industry_betas'])['industry_betas']}
    for b in plan['buckets']:
        e=existing.get(b['bucket'])
        if not e or any(abs(e[f]-v)>1e-12 for f,v in zip(['unlevered_beta','cash_adjusted_beta'],b['pair'])):
            app.save_industry_beta_changes(conn,[(b['bucket'],e['mapped_sector'] if e else b['sector'],*b['pair'],now)],[])
    session.flush()
    data=snapshot(conn,BASE);active=active_values(data)
    groups={r['name']:r['id'] for r in data['company_groups']}
    beta_names={r['user_industry_bucket'] for r in data['industry_betas']}
    beta_ids={groups[n] for n in beta_names if n in groups}
    removals=collections.defaultdict(list);adds=collections.defaultdict(list)
    for row in plan['rows']:
        cid=row['company_id'];name=row['bucket']
        if not cid or active.get(cid,(None,))[0]==name:continue
        gid=groups[name]
        for membership in data['company_group_members']:
            if membership['company_id']==cid and membership['group_id'] in beta_ids and membership['group_id']!=gid:
                removals[membership['group_id']].append(cid)
        adds[gid].append(cid)
    for gid,cids in removals.items():app.remove_company_group_members(conn,gid,cids)
    for gid,cids in adds.items():app.add_company_group_members(conn,gid,cids)
    session.flush()

def clone(data):
    conn=sqlite3.connect(':memory:')
    for table in TABLES:
        conn.execute(str(CreateTable(models.metadata.tables[table]).compile(dialect=sqlite_dialect())))
        rows=data[table]
        if rows:
            cols=list(rows[0]);conn.executemany('INSERT INTO '+table+' ('+','.join(cols)+') VALUES ('+','.join('?' for _ in cols)+')',[[r[c] for c in cols] for r in rows])
    conn.commit();return conn

def calculate_derived(data,company_ids):
    """Run unchanged app formulas against a private copy, avoiding remote N+1 reads."""
    local=clone(data)
    for cid in sorted(company_ids):
        app.compute_and_store_levered_beta(local,cid)
        app.compute_and_store_cost_of_equity(local,cid)
        app.compute_and_store_wacc(local,cid)
        app.compute_and_store_roic_wacc_spread(local,cid)
    result={}
    for table in DERIVED:
        cur=local.execute('SELECT * FROM '+table);cols=[d[0] for d in cur.description]
        result[table]=[dict(zip(cols,r)) for r in cur if r[0] in company_ids]
    local.close();return result

def validate_derived(data,derived,company_ids):
    """Independently check all eligible annual formulas, including missing outputs."""
    active=active_values(data);companies={r['id']:r for r in data['companies']}
    tax={r['country']:r['effective_rate'] for r in data['marginal_corporate_tax_rates']}
    annual=lambda name:{(r['company_id'],r['fiscal_year']):r for r in derived.get(name,data[name])}
    lb=annual('levered_beta_annual');coe=annual('cost_of_equity_annual');wacc=annual('wacc_annual');spread=annual('roic_wacc_spread_annual')
    rf={r['year']:r for r in data['risk_free_rates']};erp={r['year']:r['implied_erp'] for r in data['implied_equity_risk_premium_usa']};crp={r['year']:r for r in data['country_risk_premium']}
    country=lambda cid:companies[cid]['country']
    def tax_decimal(cid):
        rate=tax.get(country(cid),tax.get('USA'));return rate/100 if rate>1 else rate
    counts=collections.Counter()
    def check(table,key,field,expected,name):
        assert key in table and math.isclose(table[key][field],expected,rel_tol=1e-11,abs_tol=1e-11),(name,key,expected,table.get(key))
        counts[name]+=1
    for r in data['debt_equity_annual']:
        cid=r['company_id'];key=(cid,r['fiscal_year'])
        if cid in company_ids and cid in active:
            check(lb,key,'levered_beta',active[cid][1]*(1+(1-tax_decimal(cid))*r['debt_equity']),'levered_beta')
    for key,r in lb.items():
        cid,y=key
        if cid not in company_ids or y not in rf or y not in erp:continue
        rfcol={'India':'india_rf','China':'china_rf','Japan':'japan_rf'}.get(country(cid),'usa_rf')
        crpcol={'India':'india','China':'china','Japan':'japan','UK':'uk','UAE':'uae'}.get(country(cid),'us')
        expected=rf[y][rfcol]+r['levered_beta']*erp[y]+(crp.get(y,{}).get(crpcol) or 0)
        check(coe,key,'cost_of_equity',expected,'cost_of_equity')
    debt=annual('total_debt_annual');mc=annual('market_capitalization_annual');pcd=annual('pre_tax_cost_of_debt_annual')
    for key,r in debt.items():
        cid,y=key
        if cid not in company_ids or key not in mc or key not in pcd or key not in coe:continue
        d=r['total_debt'];e=mc[key]['market_capitalization']
        if d+e:
            expected=pcd[key]['pre_tax_cost_of_debt']*d*(1-tax_decimal(cid))/(d+e)+coe[key]['cost_of_equity']*e/(d+e)
            check(wacc,key,'wacc',expected,'wacc')
    for r in data['roic_direct_upload_annual']:
        key=(r['company_id'],r['fiscal_year'])
        if r['company_id'] in company_ids and key in wacc:check(spread,key,'spread_pct',r['roic_pct']-wacc[key]['wacc'],'roic_wacc_spread')
    return dict(counts)

def persist_derived(conn,derived):
    for name,rows in derived.items():
        table=models.metadata.tables[name];keys=[c.name for c in table.primary_key]
        for offset in range(0,len(rows),1500):
            stmt=pg_insert(table).values(rows[offset:offset+1500])
            stmt=stmt.on_conflict_do_update(index_elements=keys,set_={c.name:stmt.excluded[c.name] for c in table.columns if c.name not in keys})
            conn.session.execute(stmt)

def reconcile(plan,before,after):
    active=active_values(after);catnames={r['id']:r['name'] for r in after['relative_valuation_categories']}
    subs={r['id']:(catnames[r['category_id']],r['name']) for r in after['relative_valuation_subcategories']}
    ass=collections.defaultdict(list)
    for r in after['relative_valuation_company_assignments']:ass[r['company_id']].append(subs[r['subcategory_id']])
    targets={b['bucket']:b['pair'] for b in plan['buckets']};rows=[]
    for row in plan['rows']:
        r=row.copy();cid=r['company_id']
        if cid:
            r['category_ok']=ass[cid]==[(r['category'],r['subcategory'])]
            actual=active.get(cid);r['actual_bucket']=actual[0] if actual else None
            r['actual_betas']=list(actual[1:]) if actual else None
            r['industry_ok']=bool(actual and actual[0]==r['bucket'] and all(abs(a-b)<1e-12 for a,b in zip(actual[1:],targets[r['bucket']])))
            r['status']='Verified' if r['category_ok'] and r['industry_ok'] else 'MISMATCH'
        else:r['status']='Not updated: no matching issuer record'
        rows.append(r)
    assert not any(r['status']=='MISMATCH' for r in rows),'Row reconciliation failed'
    ids={r['company_id'] for r in plan['rows'] if r['company_id']}
    for table in ['relative_valuation_company_assignments','company_group_members']:
        assert ordered([r for r in before[table] if r['company_id'] not in ids])==ordered([r for r in after[table] if r['company_id'] not in ids]),'Outside associations changed: '+table
    assert ordered(before['companies'])==ordered(after['companies']),'Company identities changed'
    for b in plan['buckets']:
        defs=[r for r in after['industry_betas'] if r['user_industry_bucket']==b['bucket']]
        assert len(defs)==1 and all(abs(defs[0][f]-v)<1e-12 for f,v in zip(['unlevered_beta','cash_adjusted_beta'],b['pair']))
    return rows

def deltas(before,after):
    out={}
    for table in MUTABLE:
        pk=[c.name for c in models.metadata.tables[table].primary_key]
        key=lambda r:tuple(r[c] for c in pk)
        old={key(r):r for r in before[table]};new={key(r):r for r in after[table]}
        out[table]=[{'before':old.get(k),'after':new.get(k)} for k in sorted(old.keys()|new.keys()) if old.get(k)!=new.get(k)]
    return out

def restore(conn,delta):
    """Guard every changed row, then reverse only this migration's changes."""
    for name,changes in delta.items():
        table=models.metadata.tables[name];pk=[c.name for c in table.primary_key]
        for change in changes:
            row=change['after'] or change['before'];where=[table.c[k]==row[k] for k in pk]
            current=conn.session.execute(table.select().where(*where)).mappings().one_or_none()
            assert (dict(current) if current else None)==change['after'],'Rollback blocked by later changes: '+name+str(row)
    for name in reversed(MUTABLE):
        table=models.metadata.tables[name];pk=[c.name for c in table.primary_key]
        for change in delta[name]:
            row=change['after'] or change['before']
            if change['before'] is None:conn.session.execute(table.delete().where(*[table.c[k]==row[k] for k in pk]))
    for name in MUTABLE:
        table=models.metadata.tables[name];pk=[c.name for c in table.primary_key]
        for change in delta[name]:
            row=change['before']
            if row is None:continue
            if change['after'] is None:conn.session.execute(table.insert().values(**row))
            else:conn.session.execute(table.update().where(*[table.c[k]==row[k] for k in pk]).values(**row))

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',type=Path);parser.add_argument('--audit-dir',type=Path,required=True)
    parser.add_argument('--apply',action='store_true');parser.add_argument('--rollback',type=Path)
    args=parser.parse_args();engine=get_engine();assert engine.dialect.name=='postgresql','Refusing SQLite fallback'
    out=args.audit_dir;out.mkdir(parents=True,exist_ok=True)
    with DbCompat(SessionLocal()) as conn,conn.transaction():
        conn.execute('SET LOCAL lock_timeout = \'15s\'')
        if args.rollback:
            assert args.apply,'Use --apply for an explicit rollback'
            conn.execute('LOCK TABLE '+','.join(TABLES)+' IN SHARE ROW EXCLUSIVE MODE')
            restore(conn,json.loads(args.rollback.read_text())['deltas'])
            print('Rollback validated and committed',flush=True);return
        plan=json.loads(args.plan.read_text())
        for path,sha in plan['input_hashes'].items():assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==sha,'Input changed: '+path
        if not args.apply:
            conn.execute('SET TRANSACTION READ ONLY');print(json.dumps(plan['summary'],indent=2));return
        assert not (out/'before.json').exists(),'Use a fresh audit directory'
        conn.execute('LOCK TABLE '+','.join(TABLES)+' IN SHARE ROW EXCLUSIVE MODE')
        before=snapshot(conn)
        expected=json.loads((args.plan.parent/'snapshot.json').read_text())
        for table in BASE:assert ordered(before[table])==ordered(expected[table]),'Live assignments changed since planning: '+table
        digest=save(out/'before.json',before)
        save(out/'manifest.json',{'created_at':datetime.now(timezone.utc).isoformat(),'before_sha256':digest,'plan':plan})
        print('Backup saved and flushed before data changes:',out/'before.json',flush=True)
        apply_assignments(conn,plan)
        after=snapshot(conn,BASE)
        oldactive=active_values(before);newactive=active_values(after)
        affected={cid for cid in oldactive.keys()|newactive.keys() if oldactive.get(cid)!=newactive.get(cid)}
        calculation_data={**before,**after}
        derived=calculate_derived(calculation_data,affected)
        formula_checks=validate_derived(calculation_data,derived,affected)
        print('Calculated unchanged app formulas for',len(affected),'companies',flush=True)
        persist_derived(conn,derived);conn.session.flush()
        after.update(snapshot(conn,DERIVED))
        rows=reconcile(plan,before,after);delta=deltas(before,after)
        persisted_precision={}
        for table in DERIVED:
            persisted_precision[table]=compare_derived_rows([r for r in after[table] if r['company_id'] in affected],derived[table])
            assert ordered([r for r in before[table] if r['company_id'] not in affected])==ordered([r for r in after[table] if r['company_id'] not in affected])
        validate_derived(calculation_data,{t:[r for r in after[t] if r['company_id'] in affected] for t in DERIVED},affected)
        save(out/'after.json',after)
        save(out/'rollback.json',{'deltas':delta})
        ids={r['company_id'] for r in rows if r['company_id']}
        summary={**plan['summary'],'verified_rows':sum(r['status']=='Verified' for r in rows),'valuation_refresh_companies':len(affected),'valuation_refresh_outside_spreadsheet':len(affected-ids),'formula_checks':formula_checks,'persisted_precision':persisted_precision,'changed_rows_by_table':{t:len(r) for t,r in delta.items()},'backup_sha256':digest}
        save(out/'reconciliation.json',{'summary':summary,'rows':rows})
        print(json.dumps(summary,indent=2),flush=True)
    save(out/'COMMITTED.json',{'committed_at':datetime.now(timezone.utc).isoformat()})
    print('Committed one atomic transaction; row reconciliation passed.',flush=True)

if __name__=='__main__':main()

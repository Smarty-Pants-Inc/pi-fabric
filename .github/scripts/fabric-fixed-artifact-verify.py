"""Adapted from CI's accepted one-artifact-attempt/verify-artifact.py.
Read-only streamed archive/manifest receiving. No extraction/import/execution.
New explicit artifact/source/run bindings; original105835 artifacts untouched.
"""
import hashlib,json,posixpath,stat,tarfile,zipfile,gzip,re,time,signal,sys
from pathlib import Path,PurePosixPath
D=Path(sys.argv[1]).resolve();archive=D/'artifact-10718135458.zip';CAP=2*1024**3;MAX=200000
start=time.monotonic()
def deadline(*a):raise TimeoutError('Data verification deadline')
signal.signal(signal.SIGALRM,deadline);signal.alarm(120)
def sha(data):return hashlib.sha256(data).hexdigest()
with archive.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()=='2501a67279b6675d68bf983e9952e7980e04a9f5109171a0b46cf17c6883ebb0'
assert archive.stat().st_size==119276501
class Bounded:
 def __init__(self,stream):self.stream=stream;self.count=0
 def read(self,n=-1):
  b=self.stream.read(min(65536 if n<0 else n,CAP-self.count+1));self.count+=len(b);assert self.count<=CAP,'Uncompressed tar logical layer cap';return b
with zipfile.ZipFile(archive) as z:
 expected={'SHA256SUMS','closure-manifest.json','bun.lock','fork-source.tar','node_modules.tar.gz','pi-fabric-0.92.25.tgz','source-identities.txt','tool-digests.txt','tool-versions.txt','quickjs-tests.txt','typecheck.txt'}
 names=z.namelist();assert len(names)==len(set(names))==11 and set(names)==expected;outerLogical=sum(i.file_size for i in z.infolist());assert outerLogical<=CAP
 for i in z.infolist():assert not i.is_dir() and not stat.S_ISLNK(i.external_attr>>16) and not i.flag_bits&1 and i.compress_type in (zipfile.ZIP_STORED,zipfile.ZIP_DEFLATED)
 sums={}
 assert z.getinfo('SHA256SUMS').file_size<65536
 checksumBytes=z.read('SHA256SUMS')
 for line in checksumBytes.decode().splitlines():
  digest,name=line.split('  ',1);assert re.fullmatch('[0-9a-f]{64}',digest) and name.startswith('./');name=name[2:];assert name in expected and name!='SHA256SUMS' and name not in sums;sums[name]=digest
  with z.open(name) as f:assert hashlib.file_digest(f,'sha256').hexdigest()==digest
 assert set(sums)==expected-{'SHA256SUMS'}
 for name in ['source-identities.txt','tool-digests.txt','tool-versions.txt']:assert z.getinfo(name).file_size<=65536
 ids=dict(line.split('=',1) for line in z.read('source-identities.txt').decode().splitlines());assert ids=={'fork_commit':'f7d71b57bfc9ec7ec76fc2e02f13642ec87033e3','fork_tree':'73a4de6f00eb4dc55207b679b46eb866deb43872','smarty_dev_fixture':'private_post_receive_only','run_id':'35780469006','attempt':'1','workflow_sha':'999936a251b97a6c057cf28f2665ed471941e85b'}
 versions=z.read('tool-versions.txt');assert versions==b'node=v24.18.0\nbun=1.4.0\n'
 tools={}
 for line in z.read('tool-digests.txt').decode().splitlines():
  digest,path=line.split('  ',1);assert re.fullmatch('[0-9a-f]{64}',digest);name=PurePosixPath(path).name;assert name in ['node','bun'] and name not in tools;tools[name]={'pathAtProducer':path,'sha256':digest}
 assert set(tools)=={'node','bun'}
 assert z.getinfo('closure-manifest.json').file_size<=64*1024**2
 manifest=json.loads(z.read('closure-manifest.json'));assert isinstance(manifest,list) and len(manifest)<=MAX
 declared={r['path']:r for r in manifest};assert len(declared)==len(manifest)
 plan={};total=0;memberCount=0;layers=[]
 for filename,prefix in [('fork-source.tar','pi-fabric/'),('pi-fabric-0.92.25.tgz','package/'),('node_modules.tar.gz','')]:
  layerFiles=0;layerMembers=0
  with z.open(filename) as compressed:
   rawStream=gzip.GzipFile(fileobj=compressed,mode='rb') if filename.endswith(('.gz','.tgz')) else compressed
   bounded=Bounded(rawStream)
   with tarfile.open(fileobj=bounded,mode='r|') as tar:
    seen=set()
    for member in tar:
     memberCount+=1;layerMembers+=1;assert memberCount<=MAX
     raw=member.name.rstrip('/');assert raw not in seen;seen.add(raw)
     assert raw and '\\' not in raw and '\0' not in raw and not raw.startswith('/') and '..' not in PurePosixPath(raw).parts and raw==PurePosixPath(raw).as_posix()
     if prefix:
      if raw==prefix.rstrip('/'):assert member.isdir();continue
      assert raw.startswith(prefix);raw=raw[len(prefix):]
     else:assert raw=='node_modules' or raw.startswith('node_modules/')
     assert '.git' not in PurePosixPath(raw).parts and not member.mode&0o7000
     if member.isdir():row={'path':raw,'type':'directory','mode':0o755}
     elif member.isfile():
      assert 0<=member.size<=CAP;total+=member.size;layerFiles+=member.size;assert total<=CAP
      with tar.extractfile(member) as data:digest=hashlib.file_digest(data,'sha256').hexdigest()
      row={'path':raw,'type':'file','mode':0o755 if member.mode&0o111 else 0o644,'size':member.size,'sha256':digest}
     elif member.issym():
      target=member.linkname;resolved=posixpath.normpath(posixpath.join(posixpath.dirname(raw),target));assert target and '\\' not in target and '\0' not in target and not target.startswith('/') and resolved!='..' and not resolved.startswith('../')
      row={'path':raw,'type':'symlink','mode':0o777,'target':target}
     else:raise ValueError('Special/hardlinked tar member')
     assert raw not in plan or plan[raw]==row;plan[raw]=row;assert len(plan)<=MAX
   while bounded.read(65536):pass
   layers.append({'name':filename,'members':layerMembers,'fileBytes':layerFiles,'logicalTarBytes':bounded.count})
   if rawStream is not compressed:rawStream.close()
 for name in list(plan):
  for parent in PurePosixPath(name).parents:
   if str(parent)=='.':continue
   row={'path':str(parent),'type':'directory','mode':0o755};assert str(parent) not in plan or plan[str(parent)]==row;plan[str(parent)]=row
 assert len(plan)<=MAX and plan==declared
 # Follow chained symlinks in DATA only. Each target must remain within the
 # assembled closure and exist; no host filesystem resolution or extraction.
 symlinks=0
 for name,row in plan.items():
  if row['type']!='symlink':continue
  symlinks+=1;candidate=name;visited=set()
  for _ in range(128):
   assert candidate not in visited,'Symlink cycle';visited.add(candidate);parts=PurePosixPath(candidate).parts;changed=False
   for i in range(1,len(parts)+1):
    part='/'.join(parts[:i]);item=plan.get(part);assert item is not None,'Missing link target'
    if item['type']=='symlink':
     candidate=posixpath.normpath(posixpath.join(posixpath.dirname(part),item['target'],*parts[i:]));assert not candidate.startswith('/') and candidate!='..' and not candidate.startswith('../');changed=True;break
   if not changed:break
  else:raise ValueError('Symlink depth bound')
 result={'verified':True,'scope':'BODY_DATA_CLOSURE_ONLY','run':35780469006,'attempt':1,'job':106924410079,'artifact':10718135458,'archiveBytes':archive.stat().st_size,'archiveSHA256':'2501a67279b6675d68bf983e9952e7980e04a9f5109171a0b46cf17c6883ebb0','outerMembers':len(names),'outerLogicalBytes':outerLogical,'outerCRCAndAllSHA256SUMSVerified':True,'sourceIdentities':ids,'toolVersions':versions.decode(),'toolDigests':tools,'closureManifestSHA256':sums['closure-manifest.json'],'closureMembers':len(plan),'layerFileBytes':total,'tarMembersIncludingLayerDuplicates':memberCount,'layers':layers,'symlinksConfined':symlinks,'manifestExactEquality':True,'memberSHA256':sums,'checksumFileSHA256':sha(checksumBytes),'elapsedSeconds':time.monotonic()-start,'limits':'Each uncompressed tar/outer logical size and aggregate layer file bytes <=2GiB; <=200000 manifest and traversed entries. This is NOT a global disk-use quota.','privateFixtureRun':False,'extracted':False,'runtimeActivated':False,'old105835Untouched':True}
 (D/'ARTIFACT-VERIFIED.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
signal.alarm(0)

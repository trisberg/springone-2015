if (fsh.test(indir)) {
	fsh.rmr(indir);
}
if (fsh.test(outdir)) {
	fsh.rmr(outdir);
}
fsh.copyFromLocal(source+'/'+file, indir+'/'+file);

TGZ=/tmp/pzoo.tgz
RPO=pzk
tar czvf $TGZ Sources Tests Package.swift
scp $TGZ nut:/tmp
ssh nut "cd /tmp;rm -rf $RPO;mkdir $RPO; cd $RPO; tar xzvf $TGZ;swift build;swift test"

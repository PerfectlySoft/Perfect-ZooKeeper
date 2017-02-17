TGZ=/tmp/pzoo.tgz
RPO=PerfectZookeeper
cd ..
tar czvf $TGZ $RPO
cd $RPO
scp $TGZ nut:/tmp
ssh nut "cd /tmp;rm -rf $RPO;tar xzvf $TGZ;cd $RPO;swift build;swift test"

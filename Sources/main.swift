import LinuxBridge
import czookeeper

let z = ZooKeeper()
print("???????????  keeper start   ??????????????")
do {
  try z.connect { connection in
    print("================ CONNECT: \(connection) =============")
  }//end zooKeeper
}catch(let err) {
  print("Fault: \(err)")
}
sleep(30)

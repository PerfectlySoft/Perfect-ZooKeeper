import XCTest
@testable import PerfectZooKeeper
import Foundation
import czookeeper

class PerfectZooKeeperTests: XCTestCase {

  let path = "/zookeeper/quota/perfect"

    func testExample() {
      let x = self.expectation(description: "connection1")
      let z = ZooKeeper()
      print("???????????  keeper start   ??????????????")
      do {
        try z.connect { connection in
          XCTAssertEqual(connection, ZooKeeper.ConnectionState.CONNECTED)
          print("================ CONNECTED =============")
          x.fulfill()
        }//end zooKeeper
      }catch(let err) {
        XCTFail("Fault: \(err)")
      }
      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("time out \(err)")
        }//end if
      }//end self

      print("-------- existance & children  ------------")
      do {
        let a = try z.exists("/zookeeper")
        print(a)
        let kids = try z.children("/zookeeper")
        XCTAssertGreaterThan(kids.count, 0)
        print(kids)
      }catch (let err) {
        XCTFail("Exists Fault: \(err)")
      }//end do

      let now = time(nil)
      do {
        print("********** SYNC WRITE / READ **********")
        let s = try z.save(path, data: "你好，现在时间是 \(now)")
        print("saving result: ")
        print(s)
        let (data, stat) = try z.load(path)
        print("loading ... ")
        print(data)
        print(stat)
        let parent = path.parentPath()
        print(parent)
      }catch (let err){
        XCTFail("Load Fault: \(err)")
      }

      let writeTimer = self.expectation(description: "writing")
      print (" % % % % % % %       ASYNC WRITE  % % % % % % %")

      let written = "异步读写时间戳： \(now)"
      do {
        try z.save(path, data: written) { err, stat in
          guard err == .ZOK else {
            XCTFail("ASYNC WRITING FAULT: \(err)")
            return
          }//end guard
          guard let st = stat else {
            XCTFail("ASYNC WRITING RETURN NULL")
            return
          }
          print(st)
          writeTimer.fulfill()
        }//end save
      }catch(let err) {
        XCTFail("async save fault: \(err)")
      }

      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("writing time out \(err)")
        }//end if
      }//end self

      print (" % % % % % % %       ASYNC READ  % % % % % % %")

      let readerTimer = self.expectation(description: "reading")
      do {
        try z.load(path) { err, value, stat in
          guard err == .ZOK else {
            XCTFail("ASYNC READING FAULT: \(err)")
            return
          }//end guard
          XCTAssertEqual(written, value)
          guard let st = stat else {
            XCTFail("ASYNC READING unexpected stat")
            return
          }//end guard
          print(st)
          readerTimer.fulfill()
        }//end load
      }catch(let err) {
        XCTFail("async load fault: \(err)")
      }

      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("reading time out \(err)")
        }//end if
      }//end self

      print("$ $ $ $ $     directories     $ $ $ $ $")
      do {
        let pp = "\(path)/persistent"
        let pe = "\(path)/ephemeral"
        let ps = "\(path)/sequential"
        let pl = "\(path)/leadership"
        let rpp = try z.make(pp, value: "blah blah blah")
        print(rpp)
        XCTAssertEqual(rpp, pp)
        try z.remove(pp)
        let epp = try z.make(pe, value: "will fade away", type: .EPHEMERAL)
        XCTAssertEqual(epp, pe)
        let spp = try z.make(ps, type: .SEQUENTIAL)
        print(spp)
        try z.remove(spp)
        let lpp = try z.make(pl, type: .LEADERSHIP)
        print(lpp)
        try z.remove(lpp)
      }catch(let err) {
        XCTFail("make / remove fault: \(err)")
      }
    }

    func testGlobal() {
      ZooKeeper.debug(.ERROR)
      ZooKeeper.log()
    }

    func testUpdate() {
      let x = self.expectation(description: "connection2")
      let z = ZooKeeper()
      do {
        try z.connect { connection in
          XCTAssertEqual(connection, ZooKeeper.ConnectionState.CONNECTED)
          x.fulfill()
          print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>  PREPARE WRITING >>>>>>>>>>>>>>>>>>>>>>>>>>> ")
          for i in 0 ... 5 {
            do {
              sleep(1)
              let now = time(nil)
              let _ = try z.save(self.path, data: "🇨🇳🇨🇦异步写入监控测试： \(now)")
              print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>  \(i)  written >>>>>>>>>>>>>>>>>>>>>>>>>>> ")
            }catch (let saveErr) {
              XCTFail("write fault: \(saveErr)")
            }
          }//next
        }//end connect
      }catch(let err) {
        XCTFail("Fault: \(err)")
      }
      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("time out \(err)")
        }//end if
      }//end self
    }

    func testWatch() {
      let x = self.expectation(description: "connection3")
      var total = 0
      let z = ZooKeeper()
      do {
        try z.connect { connection in
          XCTAssertEqual(connection, ZooKeeper.ConnectionState.CONNECTED)
          do {
            try z.watch(self.path) { event in
              total += 1
              print("* * * * * * * *                                                            * * * * * * * * ")
              print("* * * * * * * *                 DETECTED #\(total): \(event)               * * * * * * * * ")
              print("* * * * * * * *                                                            * * * * * * * * ")
              if total == 3 {
                x.fulfill()
              }//end if
            }//end watch
          }catch(let err) {
            XCTFail("watch Fault: \(err)")
          }
        }//end zooKeeper
      }catch(let err) {
        XCTFail("connection ault: \(err)")
      }
      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("watcher time out \(err)")
        }//end if
      }//end self
    }

    func testElection() {
      let x = self.expectation(description: "connection4")
      let z = ZooKeeper()
      do {
        try z.connect { connection in
          XCTAssertEqual(connection, ZooKeeper.ConnectionState.CONNECTED)
          x.fulfill()
        }//end zooKeeper
      }catch(let err) {
        XCTFail("connection fault: \(err)")
      }
      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("election time out \(err)")
        }//end if
      }//end self
      print("|||||||||  ||||||||  election:                    |||||||||")
      do {
        let epath = "\(path)/election"
        for _ in 0...10 {
          let pl = try z.make(epath, type: .LEADERSHIP)
          print(pl)
        }
        let r = try z.elect(epath)
        print(r)
      }catch(let err) {
        XCTFail("election fault \(err)")
      }//end do
    }

    func show(_ aclArray: ACL_vector) {
      guard let pAcl = aclArray.data else {
        XCTFail("ACL POINTER FAULT")
        return
      }
      var i = 0
      while (Int32(i) < aclArray.count) {
        let cursor = pAcl.advanced(by: i)
        let acl = cursor.pointee
        let scheme = String(cString: acl.id.scheme)
        let id = String(cString: acl.id.id)
        print("id: \(id)")
        print("scheme: \(scheme)")
        let perm = String(format: "%8X", acl.perms)
        print("permissions: \(perm)")
        print(" - - - - -")
        let pread = String(format: "%8X", ZOO_PERM_READ)
        print("read: \(pread)")
        let pwrite = String(format: "%8X", ZOO_PERM_WRITE)
        print("write: \(pwrite)")
        let pcreate = String(format: "%8X", ZOO_PERM_CREATE)
        print("create: \(pcreate)")
        let pdelete = String(format: "%8X", ZOO_PERM_DELETE)
        print("delete: \(pdelete)")
        let padmin = String(format: "%8X", ZOO_PERM_ADMIN)
        print("admin: \(padmin)")
        let pall = String(format: "%8X", ZOO_PERM_ALL)
        print("all: \(pall)")
        i += 1
      }
    }
    func testACL() {
      print("^ ^ ^ ^ ^ ^        ACL:   ^ ^ ^ ^ ^ ^ ^")
      show(ZOO_READ_ACL_UNSAFE)
      show(ZOO_OPEN_ACL_UNSAFE)
      show(ZOO_CREATOR_ALL_ACL)
      let x = self.expectation(description: "connection5")
      let z = ZooKeeper()
      do {
        try z.connect { connection in
          XCTAssertEqual(connection, ZooKeeper.ConnectionState.CONNECTED)
          x.fulfill()
        }//end zooKeeper
      }catch(let err) {
        XCTFail("connection fault: \(err)")
      }
      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("election time out \(err)")
        }//end if
      }//end self
      let aclpath = "\(path)/acltest"
      do {
        try z.make(aclpath, type: .EPHEMERAL)
        try z.setACL(aclpath, aclTemplate: .READ)
        let (acl, _) = try z.getACL(aclpath)
        show(acl)
      }catch(let err) {
        XCTFail("acl fault: \(err)")
      }
    }
    static var allTests : [(String, (PerfectZooKeeperTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
            ("testGlobal", testGlobal),
            ("testUpdate", testUpdate),
            ("testWatch", testWatch),
            ("testACL", testACL),
            ("testElection", testElection)
        ]
    }
}

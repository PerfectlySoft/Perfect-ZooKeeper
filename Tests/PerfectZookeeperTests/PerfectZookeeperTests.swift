import XCTest
@testable import PerfectZooKeeper
import Foundation

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
        let s = try z.save(path, data: "hello, configuration \(now)")
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

      let written = "bonjour, conf \(now)"
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
        let _ = try z.make(pe, value: "will fade away")
        try z.remove(pe)
        let spp = try z.make(ps, value: "no need")
        print(spp)
        try z.remove(ps)
        let lpp = try z.make(pl)
        print(lpp)
        try z.remove(pl)
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
        }//end zooKeeper
      }catch(let err) {
        XCTFail("Fault: \(err)")
      }
      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("time out \(err)")
        }//end if
      }//end self
      for _ in 0 ... 5 {
        do {
          sleep(1)
          let now = time(nil)
          let _ = try z.save(path, data: "write to configuration \(now)")
        }catch (let err) {
          XCTFail("write fault: \(err)")
        }
      }
    }

    func testWatch() {
      let x = self.expectation(description: "connection3")
      let z = ZooKeeper()
      do {
        try z.connect { connection in
          XCTAssertEqual(connection, ZooKeeper.ConnectionState.CONNECTED)
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
      let y = self.expectation(description: "watcher")
      do {
        var total = 0
        try z.watch(path) { event in
          total += 1
          print("* * * * * * * *                                                            * * * * * * * * ")
          print("* * * * * * * *                 DETECTED #\(total): \(event)               * * * * * * * * ")
          print("* * * * * * * *                                                            * * * * * * * * ")
          if total > 3 {
            y.fulfill()
          }//end if
        }//end watch
      }catch(let err) {
        XCTFail("watch Fault: \(err)")
      }
      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("watcher time out \(err)")
        }//end if
      }//end self
    }
    static var allTests : [(String, (PerfectZooKeeperTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
            ("testGlobal", testGlobal),
            ("testUpdate", testUpdate),
            ("testWatch", testWatch)
        ]
    }
}

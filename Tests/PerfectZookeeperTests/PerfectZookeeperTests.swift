import XCTest
@testable import PerfectZooKeeper
import Foundation

class PerfectZooKeeperTests: XCTestCase {
    func testExample() {
      let x = self.expectation(description: "connection")
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
      do {
        let path = "/zookeeper/quota/perfect"
        let (data, stat) = try z.load(path)
        print(data)
        print(stat)
        let parent = path.parentPath()
        let test = try z.exists(parent)
        XCTAssertTrue(test)
        let children = try z.children(parent)
        XCTAssertGreaterThan(children.count, 0)
        print(children)
      }catch (let err){
        XCTFail("Load Fault: \(err)")
      }
    }


    static var allTests : [(String, (PerfectZooKeeperTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}

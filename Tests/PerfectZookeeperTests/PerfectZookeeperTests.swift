import XCTest
@testable import PerfectZookeeper
import Foundation

class PerfectZookeeperTests: XCTestCase {
    func testExample() {
      let x = self.expectation(description: "connection")
      let z = ZooKeeper()
      print("???????????  keeper start   ??????????????")
      do {
        try z.connect { connection in
          XCTAssertTrue(connection)
          print("================ CONNECTED =============")
          x.fulfill()
        }//end zooKeeper
      }catch(let err) {
        XCTFail("Fault: \(err)")
      }
      sleep(30)
      self.waitForExpectations(timeout: 30) { err in
        if err != nil {
          XCTFail("time out \(err)")
        }//end if
      }//end self
    }


    static var allTests : [(String, (PerfectZookeeperTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}

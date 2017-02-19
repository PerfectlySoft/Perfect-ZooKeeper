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
    }


    static var allTests : [(String, (PerfectZooKeeperTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}

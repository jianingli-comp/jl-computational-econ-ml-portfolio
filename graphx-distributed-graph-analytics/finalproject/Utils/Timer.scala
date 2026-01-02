
package ca.uwaterloo.cs451.finalproject.Utils

// Here is Timer for the project

object Timer {
    def runAndTime[T](taskName: String)(runCode: => T): T = {
        println(s"Starting: $taskName")
        val start = System.currentTimeMillis()
        val result = runCode
        val end = System.currentTimeMillis()
        val seconds = (end - start) / 1000.0

        println(f"Finished: $taskName took $seconds%.3f seconds")
        result
    }
}
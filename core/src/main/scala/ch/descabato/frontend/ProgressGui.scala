package ch.descabato.frontend

import java.awt.event.{ActionEvent, ActionListener}
import javax.swing.{Timer, UIManager}

import ch.descabato.remote.RemoteOptions
import ch.descabato.utils.Utils
import org.bridj.Pointer
import org.bridj.cpp.com.COMRuntime
import org.bridj.cpp.com.shell.ITaskbarList3
import org.bridj.jawt.JAWTUtils

import scala.collection.mutable

object CreateProgressGui {
  def apply(threads: Int, nameOfOp: String, sliderDisabled: Boolean, remoteOptions: RemoteOptions): ProgressGui = {
    if (Utils.isWindows) {
      new WindowsProgressGui(threads, nameOfOp, sliderDisabled, remoteOptions)
    } else {
      new ProgressGui(threads, nameOfOp, sliderDisabled, remoteOptions)
    }
  }
}

class ProgressGui(threads: Int, nameOfOperation: String, sliderDisabled: Boolean = false, remoteOptions: RemoteOptions) extends ActionListener {
  val mon = new ProgressMonitor(this, threads, sliderDisabled, remoteOptions)
  val timer = new Timer(40, this)
  show()

  def show() {
    var name = ""
    for (info <- UIManager.getInstalledLookAndFeels()) {
      if ("Nimbus".equals(info.getName())) {
        name = info.getClassName()
      }
    }
    if (name != "")
      UIManager.setLookAndFeel(name)
    mon.pack()
    mon.setVisible(true)
    timer.start()
  }

  def actionPerformed(e: ActionEvent) {
    val copy =
      counters.synchronized {
        counters.toList
      }
    copy.foreach {
      x =>
        x.update
        x match {
          case max: MaxValueCounter if max.name contains "Blocks" =>
            setValue(max.percent, 100)
          case max: ETACounter if max.name contains "Data Read" =>
            mon.setTitle(s"$nameOfOperation ${max.percent}%, ${max.calcEta}")
          case _ =>
        }
        val slice = getSlice(x)
        x match {
          case max: MaxValueCounter => slice.update(max.percent, 100, max.formatted)
          case x: Counter => slice.update(0, 0, x.formatted)
        }
    }
  }

  var counters: mutable.Buffer[Counter] = mutable.Buffer[Counter]()

  def pause(pausing: Boolean) {
    CLI.paused = pausing
  }

  val slices: mutable.Buffer[ProgressSlice] = mutable.Buffer[ProgressSlice]()

  def getSlice(counter: Counter): ProgressSlice = {
    slices.find(counter.name == _.getName()) match {
      case Some(x) => x
      case None =>
        val s = new ProgressSlice(counter.isInstanceOf[MaxValueCounter])
        mon.getSlices().add(s)
        mon.pack()
        slices += s
        s.setName(counter.name)
        s
    }
  }

  def add(x: Counter) {
    counters.synchronized {
      counters += x
    }
  }

  add(new MaxValueCounter {
    override def name: String = "RAM"

    maxValue = Runtime.getRuntime().maxMemory()

    override def update(): Unit = {
      this.current = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    }

    override def formatted: String = {
      s"${Utils.readableFileSize(this.current)} / ${Utils.readableFileSize(this.maxValue)}"
    }
  })

  def setValue(value: Int, max: Int) {}
}

class WindowsProgressGui(threads: Int, nameOfOperation: String, sliderDisabled: Boolean = false, remoteOptions: RemoteOptions)
    extends ProgressGui(threads, nameOfOperation, sliderDisabled, remoteOptions) {
  var list: ITaskbarList3 = _
  var hwnd: Pointer[Integer] = _
  try {
    list = COMRuntime.newInstance(classOf[ITaskbarList3])
    val hwndVal = JAWTUtils.getNativePeerHandle(mon)
    hwnd = Pointer.pointerToAddress(hwndVal).asInstanceOf[Pointer[Integer]]

  } catch {
    case x: Throwable =>
  }

  override def setValue(value: Int, max: Int) {
    if (list != null && hwnd != null)
      list.SetProgressValue(hwnd, value, max)
  }

  override def pause(pausing: Boolean) {
    super.pause(pausing)
    val flag = if (pausing) ITaskbarList3.TbpFlag.TBPF_PAUSED else ITaskbarList3.TbpFlag.TBPF_NORMAL
    list.SetProgressState(hwnd, flag)
  }

}
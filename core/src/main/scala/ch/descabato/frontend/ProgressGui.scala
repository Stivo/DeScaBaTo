package ch.descabato.frontend

import javax.swing.{Timer, UIManager}
import scala.collection.mutable.Buffer
import org.bridj.cpp.com.shell.ITaskbarList3
import org.bridj.cpp.com.COMRuntime
import org.bridj.Pointer
import org.bridj.jawt.JAWTUtils
import ch.descabato.utils.Utils
import java.awt.event.{ActionEvent, ActionListener}

object CreateProgressGui {
  def apply() = {
    if (Utils.isWindows) {
      new WindowsProgressGui()
    } else {
      new ProgressGui()
    }
  }

}

class ProgressGui extends ActionListener {
  val mon = new ProgressMonitor(this)
  val timer = new Timer(40, this)
  show()

  def show() {
    var name = ""
    for (info <- UIManager.getInstalledLookAndFeels()) {
      if ("Nimbus".equals(info.getName())) {
        name = info.getClassName();
      }
    }
    if (name != "")
      UIManager.setLookAndFeel(name)
    mon.pack()
    mon.setVisible(true)
    timer.start();
  }

  def actionPerformed(e: ActionEvent) {
    val copy =
      counters.synchronized {
        counters.toList
      }
    copy.foreach {
      x =>
        x match {
          case x: UpdatingCounter => x.update
          case max: MaxValueCounter if max.name contains "Blocks" =>
            setValue(max.percent, 100)
          case max: ETACounter if max.name contains "Data Read" =>
            mon.setTitle(s"Backing up ${max.percent}%, ${max.calcEta}")
          case _ =>
        }
        val slice = getSlice(x)
        x match {
          case max: MaxValueCounter => slice.update(max.percent, 100, max.formatted)
          case x: Counter => slice.update(0, 0, x.formatted)
        }
    }
  }

  var counters = Buffer[Counter]()

  def pause(pausing: Boolean) {
    CLI.paused = pausing;
  }

  val slices = Buffer[ProgressSlice]();

  def getSlice(x: Counter) = {
    slices.find(x.name == _.getName()) match {
      case Some(x) => x
      case None =>
        val s = new ProgressSlice(x.isInstanceOf[MaxValueCounter])
        mon.getSlices().add(s)
        mon.pack()
        slices += s
        s.setName(x.name)
        s
    }
  }

  def add(x: Counter) {
    counters.synchronized {
      counters += x
    }
  }

  def setValue(value: Int, max: Int) {}
}

class WindowsProgressGui extends ProgressGui {
  var list: ITaskbarList3 = null
  var hwnd: Pointer[Integer] = null
  try {
    list = COMRuntime.newInstance(classOf[ITaskbarList3]);
    val hwndVal = JAWTUtils.getNativePeerHandle(mon);
    hwnd = Pointer.pointerToAddress(hwndVal).asInstanceOf[Pointer[Integer]];

  } catch {
    case x: Throwable =>
  }

  override def setValue(value: Int, max: Int) {
    if (list != null && hwnd != null)
      list.SetProgressValue(hwnd, value, max);
  }

  override def pause(pausing: Boolean) {
    super.pause(pausing)
    val flag = if (pausing) ITaskbarList3.TbpFlag.TBPF_PAUSED else ITaskbarList3.TbpFlag.TBPF_NORMAL
    list.SetProgressState(hwnd, flag);
  }


}
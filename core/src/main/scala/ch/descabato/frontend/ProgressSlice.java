package ch.descabato.frontend;

import javax.swing.JPanel;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.JLabel;
import javax.swing.JProgressBar;
import javax.swing.JSplitPane;

import net.miginfocom.swing.MigLayout;

public class ProgressSlice extends JPanel {

	private JLabel lblRead;
	private JProgressBar progressBar;
	private JLabel label;
	private boolean withBar;

	/**
	 * Create the panel.
	 */
	public ProgressSlice(boolean withBar) {
		this.withBar = withBar;
		setLayout(new MigLayout("insets 0, fill"));

		lblRead = new JLabel("Read:");
		add(lblRead, "cell 0 0,width 100px!,growx 0,pushx 0");
		if (withBar) {
			progressBar = new JProgressBar();
			add(progressBar, "cell 1 0,pushx 100,growx 100");
		} else {
			label = new JLabel("Asdf");
			add(label, "cell 1 0,pushx 100,growx 100");
		}
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.lblRead.setText(name);
	}

	public void update(int cur, int maximum, String text) {
		if (withBar) {
			progressBar.setValue(cur);
			progressBar.setMaximum(maximum);
			progressBar.setStringPainted(true);
			progressBar.setString(text);
		} else {
			label.setText(text);
		}
	}

}

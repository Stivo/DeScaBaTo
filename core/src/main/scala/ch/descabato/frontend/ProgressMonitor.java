package ch.descabato.frontend;

import java.awt.BorderLayout;
import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import java.awt.FlowLayout;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JSlider;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.ImageIcon;

import ch.descabato.akka.ActorStats;
import ch.descabato.core.BackupHandler;

import java.awt.Font;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.swing.event.ChangeListener;
import javax.swing.event.ChangeEvent;

public class ProgressMonitor extends JFrame {

	private JPanel contentPane;
	private JPanel slices;

	/**
	 * Create the frame.
	 */
	public ProgressMonitor(final ProgressGui gui) {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 331);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(new BorderLayout(0, 0));
		
		JPanel panel_1 = new JPanel();
		contentPane.add(panel_1, BorderLayout.NORTH);
		panel_1.setLayout(new BoxLayout(panel_1, BoxLayout.Y_AXIS));
		
		JPanel panel_2 = new JPanel();
		panel_1.add(panel_2);
		
		JLabel lblDescabato = new JLabel("DeScaBaTo");
		lblDescabato.setFont(new Font("Tahoma", Font.PLAIN, 24));
		lblDescabato.setIcon(new ImageIcon("C:\\Users\\Stivo\\workspace-e4\\DeScaBaTo\\backup-icon.png"));
		panel_2.add(lblDescabato);
		
		JPanel panel = new JPanel();
		panel_1.add(panel);
		panel.setLayout(new BoxLayout(panel, BoxLayout.X_AXIS));
		
		final JButton btnPause = new JButton("Pause");
		btnPause.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (btnPause.getText() == "Pause") {
					btnPause.setText("Resume");
					gui.pause(true);
				} else {
					btnPause.setText("Pause");
					gui.pause(false);
				}
			}
		});
		panel.add(btnPause);
		
		final JLabel threads = new JLabel("Full speed");
		panel.add(threads);
		
		final JSlider slider = new JSlider();
		slider.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent e) {
				ActorStats.tpe().setCorePoolSize(slider.getValue());
				ActorStats.tpe().setMaximumPoolSize(slider.getValue()*2);
				threads.setText("Threads: "+slider.getValue());
			}
		});
		slider.setValue(4);
		slider.setPaintLabels(true);
		slider.setMinimum(1);
		slider.setMaximum(40);
		panel.add(slider);
		
		slices = new JPanel();
		contentPane.add(getSlices(), BorderLayout.CENTER);
		getSlices().setLayout(new BoxLayout(getSlices(), BoxLayout.Y_AXIS));
	}

	public JPanel getSlices() {
		return slices;
	}

}

package ch.descabato.frontend;

import ch.descabato.core.ActorStats;
import ch.descabato.remote.RemoteOptions;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;

public class ProgressMonitor extends JFrame {

    private final JPanel slices;

    /**
     * Create the frame.
     */
    public ProgressMonitor(final ProgressGui gui, int numthreads, boolean sliderDisabled, RemoteOptions remoteOptions) {
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setBounds(100, 100, 450, 331);
        JPanel contentPane = new JPanel();
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
        btnPause.addActionListener(e -> {
            if (btnPause.getText().equals("Pause")) {
                btnPause.setText("Resume");
                gui.pause(true);
            } else {
                btnPause.setText("Pause");
                gui.pause(false);
            }
        });
        panel.add(btnPause);

        final JLabel threads = new JLabel("Full speed");
        panel.add(threads);

        final JSlider slider = new JSlider();
        slider.addChangeListener(e -> {
            ActorStats.tpe().setCorePoolSize(slider.getValue());
            ActorStats.tpe().setMaximumPoolSize(slider.getValue() * 2);
            threads.setText("Threads: " + slider.getValue());
        });
        if (sliderDisabled)
            slider.setEnabled(false);
        slider.setValue(numthreads);
        slider.setPaintLabels(true);
        slider.setMinimum(1);
        slider.setMaximum(40);
        panel.add(slider);
        if (remoteOptions != null && remoteOptions.enabled()) {
            final JSlider uploadSpeed = new LogarithmicJSlider(10, 100000, 1000);
            uploadSpeed.addChangeListener(e -> {
                remoteOptions.setUploadSpeedLimitKiloBytesPerSecond(uploadSpeed.getValue());
                threads.setText("New Upload Speed: " + uploadSpeed.getValue() + " KB/s");
            });
            uploadSpeed.setValue(remoteOptions.getUploadSpeedLimitKiloBytesPerSecond());
            panel.add(uploadSpeed);
        }

        slices = new JPanel();
        contentPane.add(getSlices(), BorderLayout.CENTER);
        getSlices().setLayout(new BoxLayout(getSlices(), BoxLayout.Y_AXIS));
    }

    public JPanel getSlices() {
        return slices;
    }

}
